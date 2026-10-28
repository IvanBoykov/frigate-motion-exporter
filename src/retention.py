"""Which hour folders a prune pass deletes, decided from a census alone.

The policy has two tiers, and the order between them is the design:

- ``keep_days`` is a hard floor. A camera's newest ``24 * keep_days`` hour
  folders are never deleted, whatever the archive costs. The census already
  leaves them out of its candidate map, so this module could not reach them
  even by omission.
- ``keep_bytes`` is a target above that floor. It governs only what is below
  the floor, and it never overrides the floor: a floor larger than the target
  produces a reported over-budget state, not a deletion.

Deletion runs oldest hour first, across all cameras at once, which is what
keeps archive depth even: an hour disappears from one camera only where every
other camera that has that hour loses it too. A camera whose recordings stopped
long ago loses its tail first and alone, which is the intended reading of
"delete the oldest recordings" - the goal is the most footage across cameras,
not exactly one hour per camera per pass.

``now()`` is never called. Every time here comes from a key, so no clock skew
can move a deletion boundary.
"""
import logging
from collections import namedtuple

log = logging.getLogger("retention")

# Why a pass stopped where it did. Reported, never acted on: each of these
# means "the configuration or the bucket said stop", and an operator has to be
# able to tell which one from the metric alone.
REASON_TARGET_MET = "target_met"
REASON_FLOOR = "floor"
REASON_PERCENT_CAP = "percent_cap"
REASON_FOREIGN_KEYS = "foreign_keys"

PlannedFolder = namedtuple(
    "PlannedFolder", "folder_key camera epoch keys bytes"
)
CameraReport = namedtuple(
    "CameraReport",
    "keys bytes protected_hours horizon_epoch over_budget_bytes",
)
Plan = namedtuple(
    "Plan",
    "folders freed_bytes keys_freed current_bytes target_bytes "
    "headroom_bytes needed_bytes reason over_budget_bytes cameras",
)


def _empty_plan(census=None, reason="", **fields):
    if census is None:
        return Plan(
            folders=(),
            freed_bytes=0,
            keys_freed=0,
            current_bytes=0,
            target_bytes=None,
            headroom_bytes=0,
            needed_bytes=None,
            reason=reason,
            over_budget_bytes=0,
            cameras={},
            **fields,
        )
    return Plan(
        folders=(),
        freed_bytes=0,
        keys_freed=0,
        current_bytes=census.total_bytes,
        target_bytes=fields.get("target_bytes"),
        headroom_bytes=fields.get("headroom_bytes", 0),
        needed_bytes=fields.get("needed_bytes"),
        reason=reason,
        over_budget_bytes=fields.get("over_budget_bytes", 0),
        cameras=fields.get("cameras", {}),
    )


def _candidate_rows(census):
    """``(epoch, folders)`` oldest first; inside a row, largest folder first.

    Cutting a row in the middle should free the most bytes per deleted folder
    and keep the request count low. Ties break on the folder name: the operator
    allowed an arbitrary pick inside an hour, and a deterministic one makes the
    log and the tests reproducible for free.

    Each camera's newest hour folder is dropped here even though the census has
    already applied the day floor. With no day threshold configured the census
    protects nothing, and deleting a camera's newest hour would destroy the
    exact object the watermark is rebuilt from - the next start would resume
    from an older clip or from the cold-resume fallback and skip the footage in
    between. The floor subsumes this rule when it is configured; the rule is
    what keeps the floor from ever being zero.
    """
    newest = {
        camera: stat.newest_epoch
        for camera, stat in census.cameras.items()
    }

    rows = []
    for epoch in sorted(census.hours):
        folders = [
            folder
            for folder in census.hours[epoch].values()
            if folder.epoch != newest.get(folder.camera)
        ]
        if not folders:
            continue
        folders.sort(key=lambda f: (-f.bytes, f.folder_key))
        rows.append((epoch, folders))
    return rows


def _horizons(census, planned_keys):
    """Each camera's oldest surviving hour folder, with the plan applied.

    Reported as the archive's depth, and reported identically in a dry run: a
    dry run exists to be reviewed, and a horizon still showing the oldest clip
    that the plan is about to delete would review nothing.
    """
    lowest = {}
    for folders in census.hours.values():
        for folder in folders.values():
            if folder.folder_key in planned_keys:
                continue
            current = lowest.get(folder.camera)
            if current is None or folder.epoch < current:
                lowest[folder.camera] = folder.epoch

    horizons = {}
    for camera, stat in census.cameras.items():
        kept = lowest.get(camera)
        protected = stat.protected_oldest_epoch
        if kept is None:
            horizons[camera] = protected
        elif protected is None:
            horizons[camera] = kept
        else:
            horizons[camera] = min(kept, protected)
    return horizons


def _reports(census, horizons, over_budget):
    return {
        camera: CameraReport(
            keys=stat.keys,
            bytes=stat.bytes,
            protected_hours=stat.protected_hours,
            horizon_epoch=horizons.get(camera),
            over_budget_bytes=over_budget.get(camera, 0),
        )
        for camera, stat in census.cameras.items()
    }


def build_plan(
    census,
    keep_bytes=None,
    headroom_bytes=0,
    max_delete_percent=25.0,
    max_unknown_fraction=0.5,
):
    """Decide the plan for one pass over ``census``.

    Without ``keep_bytes`` the floor is the whole policy: everything the census
    handed over as a candidate is past the retention window, so all of it goes.
    With it, the pass stops as soon as the archive plus its expected growth
    fits the target. The target is compared against ``current + headroom``
    rather than ``current`` so that a target equal to a bucket quota still
    leaves room for the interval's writes.

    ``max_delete_percent`` brakes instead of vetoing: a plan that would free
    more than that share of the archive stops at the cap and the rest goes in
    the next pass. This is what makes a first enablement on an over-full bucket
    or a shrunken ``keep_days`` a sequence of bounded passes rather than one
    irreversible call.
    """
    if max_delete_percent <= 0 or max_delete_percent > 100:
        raise ValueError("max_delete_percent must be in (0, 100]")

    headroom_bytes = int(headroom_bytes)
    total = census.total_bytes
    unknown_fraction = (census.unknown_bytes / total) if total else 0.0
    if unknown_fraction > max_unknown_fraction:
        # A changed S3_PREFIX or S3_KEY_TIMEZONE makes an existing archive look
        # like foreign data. Deleting "only our keys" against such a census
        # would quietly keep the part we no longer understand forever while
        # reporting it as archived, so nothing is deleted at all.
        log.error(
            "Refusing to delete: %.1f%% of the %d bytes under the prefix are"
            " keys this archiver did not write (limit %.1f%%). A changed"
            " S3_PREFIX or S3_KEY_TIMEZONE makes an existing archive look like"
            " foreign data - fix the setting before enabling deletion.",
            unknown_fraction * 100,
            total,
            max_unknown_fraction * 100,
        )
        return _empty_plan(
            census, REASON_FOREIGN_KEYS, headroom_bytes=headroom_bytes
        )

    rows = _candidate_rows(census)

    if keep_bytes is None:
        needed_bytes = None
        target_bytes = None
        cap_bytes = int(total * max_delete_percent / 100)
    else:
        target_bytes = int(keep_bytes)
        needed_bytes = total + headroom_bytes - target_bytes
        cap_bytes = int(total * max_delete_percent / 100)
        if needed_bytes <= 0:
            return _empty_plan(
                census,
                REASON_TARGET_MET,
                target_bytes=target_bytes,
                headroom_bytes=headroom_bytes,
                needed_bytes=needed_bytes,
                cameras=_reports(census, _horizons(census, frozenset()), {}),
            )

    planned = []
    freed = 0
    keys_freed = 0
    capped = False

    for epoch, folders in rows:
        if needed_bytes is not None and freed >= needed_bytes:
            break
        for folder in folders:
            if needed_bytes is not None and freed >= needed_bytes:
                break
            if freed + folder.bytes > cap_bytes:
                capped = True
                break
            planned.append(
                PlannedFolder(
                    folder_key=folder.folder_key,
                    camera=folder.camera,
                    epoch=folder.epoch,
                    keys=folder.keys,
                    bytes=folder.bytes,
                )
            )
            freed += folder.bytes
            keys_freed += folder.keys

    planned_keys = frozenset(item.folder_key for item in planned)
    horizons = _horizons(census, planned_keys)

    shortfall = 0 if needed_bytes is None else max(needed_bytes - freed, 0)
    if capped:
        reason = REASON_PERCENT_CAP
    elif needed_bytes is not None and freed < needed_bytes:
        # Candidates exhausted and still over target: the floor is bigger than
        # the budget. Not an error and not an override - a reported state.
        reason = REASON_FLOOR
    else:
        reason = REASON_TARGET_MET

    over_total = shortfall
    per_camera = {}
    if shortfall:
        floor_bytes = sum(
            stat.protected_bytes for stat in census.cameras.values()
        )
        for camera, stat in census.cameras.items():
            if floor_bytes <= 0:
                continue
            per_camera[camera] = int(
                shortfall * stat.protected_bytes / floor_bytes
            )

    return Plan(
        folders=tuple(planned),
        freed_bytes=freed,
        keys_freed=keys_freed,
        current_bytes=total,
        target_bytes=target_bytes,
        headroom_bytes=headroom_bytes,
        needed_bytes=needed_bytes,
        reason=reason,
        over_budget_bytes=over_total,
        cameras=_reports(census, horizons, per_camera),
    )
