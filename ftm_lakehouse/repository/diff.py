"""DiffMixin - shared CDC-based diff export logic for repositories."""

from abc import abstractmethod
from datetime import datetime, timezone
from typing import Generator

from followthemoney import Statement
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
        changes: Generator[tuple[datetime, str, Statement], None, None],
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

    def _get_diff_state(self) -> tuple[int, datetime] | None:
        """Get the last diff export state (version and timestamp)."""
        state = self._tags.get(self._diff_state_key)
        if state is not None:
            return unpack_diff_name(state)
        return None

    def _set_diff_state(self, name: str) -> None:
        """Store the diff export state."""
        self._tags.put(self._diff_state_key, name)

    def export_diff(self, **kwargs) -> str | None:
        """Export only data changed since last diff export using Delta CDC.

        Uses Delta Lake Change Data Capture to identify changed entities since
        the last processed version.

        Returns:
            Created diff name (v{v}_{ts}) or None if nothing created
        """
        with self._tags.touch(self._diff_base_path) as now:
            current_version = self._statements.version
            current_timestamp = now.astimezone(timezone.utc)

            # No table yet - nothing to diff
            if current_version is None:
                return

            diff_name = pack_diff_name(current_version, current_timestamp)

            state = self._get_diff_state()
            if state is not None:
                last_version, _ = state
            else:
                last_version = None

            # No diff state yet - create initial diff by copying the full export
            # This handles cases where the Delta table is already at version > 0
            if last_version is None:
                self._write_initial_diff(current_version, current_timestamp, **kwargs)
                self._set_diff_state(diff_name)
                self.log.info(
                    f"Exported initial diff for `{self._diff_base_path}`.",
                    version=diff_name,
                )
                return diff_name

            # Nothing new since last diff
            if last_version >= current_version:
                return

            start_version = last_version + 1

            # Collect changed entity IDs from CDC
            changes = self._statements.get_changes(start_version, current_version)
            changed_entity_ids = self._filter_changes(changes)

            if not changed_entity_ids:
                return

            # Generate diff path and write (pass relative path, not URI)
            diff_uri = self._write_diff(
                changed_entity_ids, current_version, current_timestamp, **kwargs
            )

            # Update state tracking
            self._set_diff_state(diff_name)

            self.log.info(
                f"Exported {self._diff_base_path} diff.",
                version=diff_name,
                diff_uri=diff_uri,
                added_entities=len(changed_entity_ids),
            )
            return diff_name
