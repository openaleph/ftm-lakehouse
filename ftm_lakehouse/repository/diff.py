"""DiffMixin - shared CDC-based diff export logic for repositories."""

from abc import abstractmethod
from datetime import datetime, timezone
from typing import Generator

from pyarrow import timestamp
from structlog.stdlib import BoundLogger

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.storage.tags import TagStore


def unpack_diff_name(name: str) -> tuple[int, datetime]:
    """v1_YYYYMMDDTHHMMSSZ.* -> (1, datetime)"""
    name = name.split(".")[0]
    v, ts = name.split("_")
    return int(v[1:]), datetime.strptime(ts, path.TS_FORMAT)


def pack_diff_name(v: int, ts: datetime) -> str:
    """Generate diff name: v1_YYYYMMDDTHHMMSSZ"""
    return f"v{v}_{datetime.strftime(ts, path.TS_FORMAT)}"


class ParquetDiffMixin:
    """Mixin providing CDC-based diff export functionality.

    Subclasses must implement:
        - _filter_changes: method filtering CDC changes for relevant data
        - _write_diff: method writing the diff output
        - _write_initial_diff: method for writing the initial diff file
    """

    log: BoundLogger
    _tags: TagStore
    _statements: ParquetStore

    _diff_base_path: str

    @abstractmethod
    def _filter_changes(
        self,
        changes: Generator[tuple[datetime, str, dict], None, None],
    ) -> set[str]:
        """Filter CDC changes and return set of changed entity IDs."""
        ...

    @abstractmethod
    def _write_diff(self, entity_ids: set[str], v: int, ts: timestamp, **kwargs) -> str:
        """Write the diff file for the given entity IDs and return the uri to
        the diff file"""
        ...

    @abstractmethod
    def _write_initial_diff(self, version: int, ts: datetime, **kwargs) -> None:
        """Create initial diff"""
        ...

    @property
    def _diff_state_key(self) -> str:
        """Tag key for storing current diff state."""
        return f"{self._diff_base_path}-current"

    def _get_diff_state(self) -> tuple[int, int | None, datetime] | None:
        """Get the last diff export state (main_version, sidecar_version, timestamp).

        Format: v{main}_{TS}:s{sidecar}
        """
        state = self._tags.get(self._diff_state_key)
        if state is None:
            return None
        diff_part, sidecar_part = state.split(":")
        main_v, ts = unpack_diff_name(diff_part)
        sidecar_v = int(sidecar_part[1:]) if sidecar_part else None
        return main_v, sidecar_v, ts

    def _set_diff_state(self, name: str, sidecar_version: int | None = None) -> None:
        """Store the diff export state."""
        sv = f"s{sidecar_version}" if sidecar_version is not None else ""
        self._tags.put(self._diff_state_key, f"{name}:{sv}")

    def export_diff(self, **kwargs) -> str | None:
        """Export only data changed since last diff export using Delta CDC.

        Uses Delta Lake Change Data Capture to identify changed entities since
        the last processed version. Also detects sidecar-only changes (soft
        deletes) that don't produce main table CDF entries.

        Returns:
            Created diff name (v{v}_{ts}) or None if nothing created
        """
        with self._tags.touch(self._diff_base_path) as now:
            current_version = self._statements.version
            current_sidecar_version = self._statements.sidecar_version
            current_timestamp = now.astimezone(timezone.utc)

            # No table yet - nothing to diff
            if current_version is None:
                return

            diff_name = pack_diff_name(current_version, current_timestamp)

            state = self._get_diff_state()
            if state is not None:
                last_version, last_sidecar_version, _ = state
            else:
                last_version = None
                last_sidecar_version = None

            # No diff state yet - create initial diff by copying the full export
            # This handles cases where the Delta table is already at version > 0
            if last_version is None:
                self._write_initial_diff(current_version, current_timestamp, **kwargs)
                self._set_diff_state(diff_name, current_sidecar_version)
                self.log.info(
                    f"Exported initial diff for `{self._diff_base_path}`.",
                    version=diff_name,
                )
                return diff_name

            # Check if anything changed (main table or sidecar)
            main_changed = last_version < current_version
            sidecar_changed = current_sidecar_version is not None and (
                last_sidecar_version is None
                or last_sidecar_version < current_sidecar_version
            )

            if not main_changed and not sidecar_changed:
                return

            # Collect changed entity IDs from main table CDC
            changed_entity_ids: set[str] = set()
            if main_changed:
                start_version = last_version + 1
                changes = self._statements.get_changes(start_version, current_version)
                changed_entity_ids = self._filter_changes(changes)

            # Also include entities that were soft-deleted via sidecar
            if sidecar_changed:
                deleted_ids = self._statements.get_deleted_entity_ids()
                changed_entity_ids |= deleted_ids

            if not changed_entity_ids:
                return

            # Generate diff path and write (pass relative path, not URI)
            diff_uri = self._write_diff(
                changed_entity_ids, current_version, current_timestamp, **kwargs
            )

            # Update state tracking
            self._set_diff_state(diff_name, current_sidecar_version)

            self.log.info(
                f"Exported {self._diff_base_path} diff.",
                version=diff_name,
                diff_uri=diff_uri,
                added_entities=len(changed_entity_ids),
            )
            return diff_name
