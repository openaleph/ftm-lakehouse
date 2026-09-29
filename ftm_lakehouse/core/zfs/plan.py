"""Replication decisions: what to send, and whether a target may take it.

Pure functions over ``zfs_agent.Status`` – no ``zfs`` calls, so the rules
that keep a receive from destroying data are testable as a table.
"""

from dataclasses import dataclass

from zfs_agent import Status


@dataclass
class Transfer:
    """What to send so a target catches up: a snapshot, incremental from
    ``since`` if set – or a resume ``token`` alone. ``base`` is the guid the
    stream applies on top of (``None`` for a full stream); the receiving side
    checks it with `check_target` before it takes a byte."""

    snapshot: str | None = None
    since: str | None = None
    token: str | None = None
    base: str | None = None


def check_target(
    target: Status, base: str | None, force: bool = False, replace: bool = False
) -> None:
    """Refuse a receive onto ``target`` that would destroy what it mustn't.

    Every receive runs ``zfs receive -F``, which rolls the target back to
    the stream's base: changes made since are discarded – that is what lets
    a merely mounted replica receive – and so is every *snapshot* after the
    base, which needs ``force``. A full stream (no ``base``) replaces a
    target that exists without snapshots, which needs ``replace``. Run by
    the receiving side right before it receives, so it is checked against
    the target as it is, whatever the sender planned with.

    Args:
        target: Status of the receiving dataset.
        base: Guid of the snapshot the stream applies on top of, ``None``
            for a full stream.
        force: Allow destroying target snapshots newer than ``base``.
        replace: Allow a full stream to replace a target without snapshots.

    Raises:
        ValueError: when the receive is refused.
    """
    snapshots = target["snapshots"]
    if base is None:
        if snapshots:
            raise ValueError("Source and target share no snapshot")
        # An interrupted receive that was creating the dataset leaves it
        # behind, snapshotless, with the token: resuming only finishes a
        # full receive that was let in already.
        if target["exists"] and not target["resume_token"] and not replace:
            raise ValueError(
                "Target exists without snapshots – a full receive would "
                "replace its contents (use replace)"
            )
        return
    guids = [s["guid"] for s in snapshots]
    if base not in guids:
        raise ValueError("Target doesn't have the incremental base snapshot")
    newer = [s["name"] for s in snapshots[guids.index(base) + 1 :]]
    if newer and not force:
        raise ValueError(
            "Target has snapshots newer than the common base that the "
            f"receive would destroy: {', '.join(newer)} (use force)"
        )


def common_base(source: Status, target: Status) -> str | None:
    """Guid of the newest source snapshot the target has too."""
    guids = {s["guid"] for s in target["snapshots"]}
    common = [s["guid"] for s in source["snapshots"] if s["guid"] in guids]
    return common[-1] if common else None


def plan_transfer(
    source: Status,
    target: Status,
    snapshot: str,
    force: bool = False,
    replace: bool = False,
) -> Transfer | None:
    """Decide how ``target`` gets to ``source@snapshot``.

    Snapshots are matched by guid, not by name, so any snapshot both sides
    share – whoever took and replicated it – can be the incremental base.
    A pending resume token is resumed rather than started over, but only if
    the resumed stream passes `check_target` too – the target may have got
    new snapshots since it was interrupted.

    Args:
        source: Status of the sending dataset.
        target: Status of the receiving dataset.
        snapshot: Name of the source snapshot to bring the target to.
        force: See `check_target`.
        replace: See `check_target`.

    Returns:
        The transfer, or ``None`` if the target already has the snapshot. A
        resume comes first – plan again once it's done.

    Raises:
        ValueError: when the source lacks ``snapshot``, the target has a
            different snapshot of that name, or `check_target` refuses.
    """
    names = [s["name"] for s in source["snapshots"]]
    if snapshot not in names:
        raise ValueError(f"Source has no snapshot `{snapshot}`")
    history = source["snapshots"][: names.index(snapshot) + 1]
    wanted = history[-1]
    target_guids = {s["guid"] for s in target["snapshots"]}
    if wanted["guid"] in target_guids and not target["resume_token"]:
        return None
    if any(
        s["name"] == snapshot and s["guid"] != wanted["guid"]
        for s in target["snapshots"]
    ):
        raise ValueError(
            f"Target has a different snapshot named `{snapshot}` – zfs won't "
            "receive over it"
        )
    common = [s for s in history if s["guid"] in target_guids]
    base = common[-1] if common else None
    base_guid = base["guid"] if base else None
    check_target(target, base_guid, force, replace)
    if target["resume_token"]:
        return Transfer(token=target["resume_token"], base=base_guid)
    return Transfer(
        snapshot=snapshot, since=base["name"] if base else None, base=base_guid
    )
