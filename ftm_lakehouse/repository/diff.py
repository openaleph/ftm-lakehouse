"""DiffMixin - diff export logic for repositories."""

from abc import abstractmethod
from datetime import datetime, timezone
from itertools import chain
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

    _diff_base_path: str

    @property
    def _statements(self) -> ParquetStore:
        """Provided by the implementing repository (a ``cached_property``)."""
        raise NotImplementedError

    @abstractmethod
    def _get_changed_ids(self, since: datetime) -> Iterator[str]:
        """Get entity IDs with statements added since the given timestamp."""
        ...

    @abstractmethod
    def _write_diff(self, entity_ids: Iterator[str], ts: datetime) -> tuple[str, int]:
        """Write the diff file for the given changed entity ids.

        ``entity_ids`` streams in from `_get_changed_ids`; the impl owns
        the one materialization the DEL derivation needs (it has to know which
        changed ids produced no live entity), which is also where the returned
        count comes from.

        Returns:
            ``(uri of the written diff, number of changed entities)``.
        """
        ...

    @abstractmethod
    def _write_initial_diff(self, ts: datetime) -> None:
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

    def export_diff(self) -> str | None:
        """Export only the data changed since the last diff export.

        Changed entities are identified by their statements' ``first_seen``;
        soft deletes surface through ``deleted_at`` on the raw statement
        view (`ParquetStore.source_raw`). Each changed entity is then
        re-read *whole*, so an ADD carries its current state.

        Requires an optimized store: reads are canonical only after ``merge``
        (the live view does no read-time dedupe), and change detection reads
        ``first_seen``, which ``merge`` folds per statement id.

        Returns:
            Timestamp string of the created diff, or None if nothing created

        Raises:
            RuntimeError: If the statement store has un-merged writes.
        """
        with self._tags.touch(self._diff_base_path) as now:
            current_version = self._statements.version

            # No table yet - nothing to diff
            if current_version is None:
                return

            # A diff publishes each changed entity's current state, which the
            # live view only reports correctly once `merge` has collapsed
            # duplicates and applied tombstones. On an un-merged store it
            # would emit superseded values and miss deletes - refuse rather
            # than publish a wrong delta.
            if self._statements.needs_merge:
                raise RuntimeError(
                    f"Cannot export `{self._diff_base_path}`: the statement "
                    "store has un-merged writes and a diff publishes canonical "
                    "entities. Run `ftm-lakehouse maintenance optimize` first."
                )

            state = self._get_diff_state()

            # No prior state – create initial diff
            if state is None:
                self._write_initial_diff(now)
                self._set_diff_state(now, current_version)
                ts_label = now.strftime(path.TS_FORMAT)
                self.log.info(
                    f"Exported initial diff for `{self._diff_base_path}`.",
                    version=ts_label,
                )
                return ts_label

            last_timestamp, last_version = state

            main_changed = last_version < current_version

            if not main_changed:
                return

            # Changed entity IDs stream straight into the writer. Peek one
            # first: if the version bumped but no entity has a new first_seen
            # >= last_timestamp (e.g. ``merge`` folded ``first_seen`` back)
            # there's no diff content, and no file should be opened for it.
            changed_ids = self._get_changed_ids(last_timestamp)
            first = next(changed_ids, None)
            if first is None:
                self._set_diff_state(now, current_version)
                return

            diff_uri, changed = self._write_diff(chain([first], changed_ids), now)

            self._set_diff_state(now, current_version)

            ts_label = now.strftime(path.TS_FORMAT)
            self.log.info(
                f"Exported {self._diff_base_path} diff.",
                version=ts_label,
                diff_uri=mask_uri(diff_uri),
                added_entities=changed,
            )
            return ts_label
