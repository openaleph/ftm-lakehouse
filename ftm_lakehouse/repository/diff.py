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

    A subclass that supports origin-scoped diffs additionally overrides
    `_get_diff_base_path` – each scope keeps its own paths, freshness tag and
    diff state, so scopes advance independently.
    """

    log: BoundLogger
    _tags: TagStore

    _diff_base_path: str

    @property
    def _statements(self) -> ParquetStore:
        """Provided by the implementing repository (a ``cached_property``)."""
        raise NotImplementedError

    @abstractmethod
    def _get_changed_ids(
        self, since: datetime, origin: str | None = None
    ) -> Iterator[str]:
        """Get entity IDs with statements added since the given timestamp."""
        ...

    @abstractmethod
    def _write_diff(
        self, entity_ids: Iterator[str], ts: datetime, origin: str | None = None
    ) -> tuple[str, int]:
        """Write the diff file for the given changed entity ids.

        ``entity_ids`` streams in from `_get_changed_ids`; the impl owns
        the one materialization the DEL derivation needs (it has to know which
        changed ids produced no live entity), which is also where the returned
        count comes from.

        Returns:
            ``(uri of the written diff, number of changed entities)``.
        """
        ...

    def _get_diff_base_path(self, origin: str | None = None) -> str:
        """Base path of the diff variant – its freshness tag and state key.

        Args:
            origin: Source tag the diff is scoped to.

        Raises:
            NotImplementedError: when the repository has no origin-scoped
                diffs (the default – only `DocumentRepository` overrides this).
        """
        if origin:
            raise NotImplementedError(
                f"`{self._diff_base_path}` diffs are not origin-scoped."
            )
        return self._diff_base_path

    def _diff_state_key(self, origin: str | None = None) -> str:
        """Tag key for storing current diff state."""
        return f"{self._get_diff_base_path(origin)}-current"

    def _get_diff_state(self, origin: str | None = None) -> tuple[datetime, int] | None:
        """Get last diff state: (timestamp, version).

        Format: {TS}:{version}
        """
        state = self._tags.get(self._diff_state_key(origin))
        if state is None:
            return None
        ts_str, main_v = state.split(":")
        return (
            datetime.strptime(ts_str, path.TS_FORMAT).replace(tzinfo=timezone.utc),
            int(main_v),
        )

    def _set_diff_state(
        self, ts: datetime, version: int, origin: str | None = None
    ) -> None:
        """Store the diff export state."""
        ts_str = ts.strftime(path.TS_FORMAT)
        self._tags.put(self._diff_state_key(origin), f"{ts_str}:{version}")

    def export_diff(self, origin: str | None = None) -> str | None:
        """Export only the data changed since the last diff export.

        Changed entities are identified by their statements' ``first_seen``;
        soft deletes surface through ``deleted_at`` on the raw statement
        view (`ParquetStore.source_raw`). Each changed entity is then
        re-read *whole*, so an ADD carries its current state.

        The first run writes no file – it only records the state the next diff
        is taken against. The full picture at that point is the export itself
        (``documents.csv`` / ``entities.ftm.json``), so a consumer starts from
        that and follows the diffs; a copy of it under ``diffs/`` would be the
        same bytes under a second name.

        Requires an optimized store: reads are canonical only after ``merge``
        (the live view does no read-time dedupe), and change detection reads
        ``first_seen``, which ``merge`` folds per statement id.

        Args:
            origin: Only diff statements from this source tag, into the
                origin-scoped diff paths. ``None`` covers every origin.

        Returns:
            Timestamp string of the created diff, or None if nothing created

        Raises:
            RuntimeError: If the statement store has un-merged writes.
            NotImplementedError: If ``origin`` is given and the repository has
                no origin-scoped diffs.
        """
        base_path = self._get_diff_base_path(origin)
        with self._tags.touch(base_path) as now:
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
                    f"Cannot export `{base_path}`: the statement "
                    "store has un-merged writes and a diff publishes canonical "
                    "entities. Run `ftm-lakehouse maintenance optimize` first."
                )

            state = self._get_diff_state(origin)

            # No prior state – only record where the next diff starts
            if state is None:
                self._set_diff_state(now, current_version, origin)
                ts_label = now.strftime(path.TS_FORMAT)
                self.log.info(
                    f"Initialized diff state for `{base_path}`.",
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
            changed_ids = self._get_changed_ids(last_timestamp, origin)
            first = next(changed_ids, None)
            if first is None:
                self._set_diff_state(now, current_version, origin)
                return

            diff_uri, changed = self._write_diff(
                chain([first], changed_ids), now, origin
            )

            self._set_diff_state(now, current_version, origin)

            ts_label = now.strftime(path.TS_FORMAT)
            self.log.info(
                f"Exported {base_path} diff.",
                version=ts_label,
                diff_uri=mask_uri(diff_uri),
                added_entities=changed,
            )
            return ts_label
