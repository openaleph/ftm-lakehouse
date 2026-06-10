"""DiffMixin - diff export logic for repositories."""

from abc import abstractmethod
from datetime import datetime, timezone
from typing import Iterator

from anystore.types import SDict
from anystore.util import mask_uri
from structlog.stdlib import BoundLogger

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.storage.parquet import ParquetStore
from ftm_lakehouse.storage.tags import TagStore


def make_envelope(data: SDict, op: str = "ADD") -> SDict:
    """Create a diff action envelope for an entity payload.

    Ref. https://www.opensanctions.org/docs/bulk/delta/
    """
    return {"op": op, "entity": data}


class ParquetDiffMixin:
    """Mixin providing diff export functionality.

    Uses the Statements first_seen timestamps to detect changed entities

    Subclasses must implement:
        - _get_changed_ids: get entity IDs changed since a timestamp
        - _write_diff: write the diff output
        - _write_initial_diff: write the initial diff file
    """

    log: BoundLogger
    _tags: TagStore
    _statements: ParquetStore

    _diff_base_path: str

    @abstractmethod
    def _get_changed_ids(self, since: datetime) -> Iterator[str]:
        """Get entity IDs with statements added since the given timestamp."""
        ...

    @abstractmethod
    def _write_diff(self, entity_ids: Iterator[str], ts: datetime, **kwargs) -> str:
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

    def _get_diff_state(self) -> tuple[datetime, int] | None:
        """Get last diff state: (timestamp, version).

        Format: {TS}:{version}
        """
        state = self._tags.get(self._diff_state_key)
        if state is None:
            return None
        ts_str, main_v = state.split(":")
        return (
            datetime.strptime(ts_str, path.TS_FORMAT).replace(tzinfo=timezone.utc),
            int(main_v),
        )

    def _set_diff_state(self, ts: datetime, version: int) -> None:
        """Store the diff export state."""
        ts_str = ts.strftime(path.TS_FORMAT)
        self._tags.put(self._diff_state_key, f"{ts_str}:{version}")

    def export_diff(self, **kwargs) -> str | None:
        """Export only data changed since last diff export using the translog.

        Uses the translog's first_seen timestamps to identify changed entities
        since the last export. Also detects soft deletes via translog deleted_at.

        Returns:
            Timestamp string of the created diff, or None if nothing created
        """
        with self._tags.touch(self._diff_base_path) as now:
            current_version = self._statements.version
            current_timestamp = now.astimezone(timezone.utc)

            # No table yet - nothing to diff
            if current_version is None:
                return

            state = self._get_diff_state()

            # No prior state – create initial diff
            if state is None:
                self._write_initial_diff(current_timestamp, **kwargs)
                self._set_diff_state(current_timestamp, current_version)
                ts_label = current_timestamp.strftime(path.TS_FORMAT)
                self.log.info(
                    f"Exported initial diff for `{self._diff_base_path}`.",
                    version=ts_label,
                )
                return ts_label

            last_timestamp, last_version = state

            # Check if anything changed (main table or translog)
            main_changed = last_version < current_version

            if not main_changed:
                return

            # Collect changed entity IDs. If the version bumped but no entity
            # has new first_seen >= last_timestamp (e.g. ``merge`` folded
            # ``first_seen`` back), there's no diff content to write.
            changed_entity_ids = list(self._get_changed_ids(last_timestamp))
            if not changed_entity_ids:
                self._set_diff_state(current_timestamp, current_version)
                return

            diff_uri = self._write_diff(
                iter(changed_entity_ids), current_timestamp, **kwargs
            )

            self._set_diff_state(current_timestamp, current_version)

            ts_label = current_timestamp.strftime(path.TS_FORMAT)
            self.log.info(
                f"Exported {self._diff_base_path} diff.",
                version=ts_label,
                diff_uri=mask_uri(diff_uri),
                added_entities=len(changed_entity_ids),
            )
            return ts_label
