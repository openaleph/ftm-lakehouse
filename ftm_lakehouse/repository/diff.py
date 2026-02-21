"""DiffMixin - translog-based diff export logic for repositories."""

from abc import abstractmethod
from datetime import datetime, timezone

from anystore.util import mask_uri
from structlog.stdlib import BoundLogger

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.storage.tags import TagStore


class ParquetDiffMixin:
    """Mixin providing translog-based diff export functionality.

    Uses the translog's first_seen timestamps to detect changed entities,
    replacing the previous CDF-based approach (which broke after compact+vacuum).

    Subclasses must implement:
        - _get_changed_ids_from_translog: get entity IDs changed since a timestamp
        - _write_diff: write the diff output
        - _write_initial_diff: write the initial diff file
    """

    log: BoundLogger
    _tags: TagStore
    _statements: ParquetStore

    _diff_base_path: str

    @abstractmethod
    def _get_changed_ids_from_translog(self, since: datetime) -> set[str]:
        """Get entity IDs with statements added since the given timestamp."""
        ...

    @abstractmethod
    def _write_diff(self, entity_ids: set[str], ts: datetime, **kwargs) -> str:
        """Write the diff file for the given entity IDs and return the uri to
        the diff file."""
        ...

    @abstractmethod
    def _write_initial_diff(self, ts: datetime, **kwargs) -> None:
        """Create initial diff."""
        ...

    @property
    def _diff_state_key(self) -> str:
        """Tag key for storing current diff state."""
        return f"{self._diff_base_path}-current"

    def _get_diff_state(self) -> tuple[datetime, int, int | None] | None:
        """Get last diff state: (timestamp, main_version, translog_version).

        Format: {TS}:{main_version}:{translog_version}
        """
        state = self._tags.get(self._diff_state_key)
        if state is None:
            return None
        parts = state.split(":")
        if len(parts) != 3:
            # Old format (v{ver}_{ts}:s{sidecar_ver}) — reset to initial diff
            self.log.info("Resetting diff state from legacy format.", old_state=state)
            return None
        ts_str, main_v, translog_v = parts
        return (
            datetime.strptime(ts_str, path.TS_FORMAT).replace(tzinfo=timezone.utc),
            int(main_v),
            int(translog_v) if translog_v else None,
        )

    def _set_diff_state(
        self, ts: datetime, main_version: int, translog_version: int | None
    ) -> None:
        """Store the diff export state."""
        ts_str = ts.strftime(path.TS_FORMAT)
        sv = str(translog_version) if translog_version is not None else ""
        self._tags.put(self._diff_state_key, f"{ts_str}:{main_version}:{sv}")

    def export_diff(self, **kwargs) -> str | None:
        """Export only data changed since last diff export using the translog.

        Uses the translog's first_seen timestamps to identify changed entities
        since the last export. Also detects soft deletes via translog deleted_at.

        Returns:
            Timestamp string of the created diff, or None if nothing created
        """
        with self._tags.touch(self._diff_base_path) as now:
            current_version = self._statements.version
            current_translog_version = self._statements.translog_version
            current_timestamp = now.astimezone(timezone.utc)

            # No table yet - nothing to diff
            if current_version is None:
                return

            state = self._get_diff_state()

            # No prior state — create initial diff
            if state is None:
                self._write_initial_diff(current_timestamp, **kwargs)
                self._set_diff_state(
                    current_timestamp, current_version, current_translog_version
                )
                ts_label = current_timestamp.strftime(path.TS_FORMAT)
                self.log.info(
                    f"Exported initial diff for `{self._diff_base_path}`.",
                    version=ts_label,
                )
                return ts_label

            last_timestamp, last_version, last_translog_version = state

            # Check if anything changed (main table or translog)
            main_changed = last_version < current_version
            translog_changed = current_translog_version is not None and (
                last_translog_version is None
                or last_translog_version < current_translog_version
            )

            if not main_changed and not translog_changed:
                return

            # Collect changed entity IDs from translog timestamps
            changed_entity_ids: set[str] = set()
            if main_changed:
                changed_entity_ids = self._get_changed_ids_from_translog(last_timestamp)

            # Also include entities that were soft-deleted via translog
            if translog_changed:
                deleted_ids = self._statements.get_deleted_entity_ids()
                changed_entity_ids |= deleted_ids

            if not changed_entity_ids:
                return

            diff_uri = self._write_diff(changed_entity_ids, current_timestamp, **kwargs)

            self._set_diff_state(
                current_timestamp, current_version, current_translog_version
            )

            ts_label = current_timestamp.strftime(path.TS_FORMAT)
            self.log.info(
                f"Exported {self._diff_base_path} diff.",
                version=ts_label,
                diff_uri=mask_uri(diff_uri),
                added_entities=len(changed_entity_ids),
            )
            return ts_label
