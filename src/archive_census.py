"""One full listing of the archive prefix, reduced to per-(camera, hour) sizes.

Retention is the only consumer that has to see the whole archive: an age- or
size-based decision cannot be made from the bounded window the watermark scan
uses. The listing is therefore the expensive part of a prune pass, and this
module keeps what it holds in memory proportional to the number of hour
folders, never to the number of clips.

Two properties make that possible, and both come from S3 rather than from this
code:

- ``list_objects_v2`` returns keys in lexicographic order, and the key layout
  (``<camera>/YYYY/MM/DD/HH/...``, zero padded) sorts a camera's hour folders
  oldest first. Each camera's hour folders therefore stream in chronological
  order.
- An hour folder is provably older than a camera's floor once that camera has
  ``floor_hours`` newer hour folders behind it. Streaming oldest-first, that is
  exactly the moment the oldest hour falls out of a deque holding the last
  ``floor_hours`` hours - so a protected hour is never offered as a candidate
  and never retained at all.

Individual keys are never kept either: the deleter re-lists the few prefixes a
plan names, which is both cheaper and the check that the plan is still valid.
"""
import logging
import re
from collections import deque, namedtuple
from datetime import datetime

import frigate_s3_archiver as archiver

log = logging.getLogger("archive_census")

# What a key under the prefix is allowed to be. Anything else is foreign: its
# bytes count towards the archive size (we pay for them either way) and it is
# never a delete candidate.
OURS = "ours"
UNKNOWN = "unknown"

HourFolder = namedtuple(
    "HourFolder", "camera folder_key epoch keys bytes"
)
CameraStat = namedtuple(
    "CameraStat",
    "keys bytes newest_epoch oldest_epoch protected_hours protected_bytes "
    "protected_oldest_epoch recent_hour_bytes",
)
Census = namedtuple(
    "Census",
    "total_bytes total_keys unknown_bytes unknown_keys hours cameras "
    "newest_epoch listed_prefix",
)


def build_pattern(key_timezone):
    """The key layout regex, as the inverse of ``make_s3_key``.

    Same three window-closing shapes ``parse_s3_clip_start`` accepts (plain
    clip, truncated clip, no-recordings marker), so a changed
    ``S3_PREFIX``/``S3_KEY_TIMEZONE`` shows up as foreign bytes - which the
    caller can refuse to delete against - rather than as an archive with a
    surprising layout.
    """
    closed_clip = re.escape(archiver.TRUNCATED_KEY_SUFFIX) + re.escape(
        archiver.CLIP_KEY_EXTENSION
    )
    closed_marker = re.escape(archiver.UNAVAILABLE_KEY_SUFFIX) + re.escape(
        archiver.NO_RECORDINGS_KEY_EXTENSION
    )
    return re.compile(
        r"^(?P<camera>[^/]+)/"
        r"(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/(?P<hour>\d{2})/"
        r"(?P<minute>\d{2})-(?P<second>\d{2})"
        r"(?:"
        + re.escape(archiver.CLIP_KEY_EXTENSION)
        + "|"
        + closed_clip
        + "|"
        + closed_marker
        + ")$"
    )


def classify(key, prefix, pattern, key_timezone):
    """``(kind, camera, hour_folder_key, hour_epoch)`` for one key.

    ``hour_epoch`` is derived from the folder's own date and hour, not floored
    from the clip timestamp: the folder is the unit of deletion, and flooring a
    timestamp in a half-hour zone could split one folder across two hours.
    """
    if not key.startswith(prefix):
        return UNKNOWN, None, None, None
    match = pattern.match(key[len(prefix) :])
    if match is None:
        return UNKNOWN, None, None, None

    parts = match.groupdict()
    camera = parts["camera"]
    try:
        moment = datetime(
            int(parts["year"]),
            int(parts["month"]),
            int(parts["day"]),
            int(parts["hour"]),
            tzinfo=key_timezone,
        )
    except ValueError:
        return UNKNOWN, camera, None, None

    folder = (
        f"{prefix}{camera}/"
        f"{parts['year']}/{parts['month']}/{parts['day']}/{parts['hour']}/"
    )
    return OURS, camera, folder, int(moment.timestamp())


def _new_camera():
    return {
        "keys": 0,
        "bytes": 0,
        "newest": None,
        "oldest": None,
        "floor": deque(),
        "recent": deque(),
        "open_folder": None,
    }


def _offer(hours, folder):
    hours.setdefault(folder.epoch, {})[folder.folder_key] = folder


def _close_hour(state, hours, floor_hours, recent_hours):
    """Finish the hour folder being accumulated for one camera.

    Overflowing the floor window is what makes an hour deletable: the evicted
    hour is the one with ``floor_hours`` newer hours behind it.
    """
    folder = state["open_folder"]
    state["open_folder"] = None
    if folder is None:
        return

    state["keys"] += folder.keys
    state["bytes"] += folder.bytes
    if state["newest"] is None or folder.epoch > state["newest"]:
        state["newest"] = folder.epoch
    if state["oldest"] is None or folder.epoch < state["oldest"]:
        state["oldest"] = folder.epoch

    if recent_hours > 0:
        state["recent"].append(folder)
        if len(state["recent"]) > recent_hours:
            state["recent"].popleft()

    if floor_hours <= 0:
        _offer(hours, folder)
        return

    state["floor"].append(folder)
    if len(state["floor"]) > floor_hours:
        _offer(hours, state["floor"].popleft())


def take_census(
    s3_client,
    bucket,
    prefix="",
    key_timezone=None,
    floor_hours=0,
    recent_hours=0,
):
    """List every key under ``prefix`` once and reduce it to hour folders.

    ``floor_hours`` is ``24 * KEEP_DAYS``: a camera's newest that many hour
    folders are protected, never offered as candidates. Zero protects nothing,
    which is the mode with no day threshold - the caller still has each
    camera's newest hour in ``cameras[camera].newest_epoch`` to protect.

    ``recent_hours`` asks for the byte sizes of each camera's newest that many
    hours, which is what the caller needs to predict near-future growth.

    A listing failure propagates: a partial census would under-report the
    archive's size and hand the planner a plan about footage it never saw.
    """
    if key_timezone is None:
        key_timezone = archiver.DEFAULT_S3_KEY_TIMEZONE
    if floor_hours < 0 or recent_hours < 0:
        raise ValueError("floor_hours and recent_hours must be >= 0")

    pattern = build_pattern(key_timezone)
    hours = {}
    cameras = {}
    total_bytes = 0
    total_keys = 0
    unknown_bytes = 0
    unknown_keys = 0
    newest_epoch = None

    paginator = s3_client.get_paginator("list_objects_v2")
    log.debug(
        "Census: listing %s in %s", prefix or "(bucket root)", bucket
    )
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            size = int(obj.get("Size", 0) or 0)
            total_keys += 1
            total_bytes += size

            kind, camera, folder_key, epoch = classify(
                key, prefix, pattern, key_timezone
            )
            if kind is UNKNOWN:
                unknown_keys += 1
                unknown_bytes += size
                continue

            state = cameras.get(camera)
            if state is None:
                state = _new_camera()
                cameras[camera] = state

            open_folder = state["open_folder"]
            if open_folder is not None and open_folder.folder_key == folder_key:
                state["open_folder"] = open_folder._replace(
                    keys=open_folder.keys + 1, bytes=open_folder.bytes + size
                )
            else:
                _close_hour(state, hours, floor_hours, recent_hours)
                state["open_folder"] = HourFolder(
                    camera, folder_key, epoch, 1, size
                )

            if newest_epoch is None or epoch > newest_epoch:
                newest_epoch = epoch

    for state in cameras.values():
        _close_hour(state, hours, floor_hours, recent_hours)

    stats = {}
    for camera, state in cameras.items():
        # The final flush closed the last folder into the window, so what is
        # left in the deque is exactly the protected tail.
        protected = list(state["floor"])
        stats[camera] = CameraStat(
            keys=state["keys"],
            bytes=state["bytes"],
            newest_epoch=state["newest"],
            oldest_epoch=state["oldest"],
            protected_hours=len(protected),
            protected_bytes=sum(folder.bytes for folder in protected),
            protected_oldest_epoch=(
                protected[0].epoch if protected else None
            ),
            recent_hour_bytes=[folder.bytes for folder in state["recent"]],
        )

    log.info(
        "Census: %d keys / %.2f GiB under %s - %d cameras, %d candidate hours,"
        " %d foreign keys (%.2f GiB)",
        total_keys,
        total_bytes / 2**30,
        prefix or "(bucket root)",
        len(stats),
        sum(len(row) for row in hours.values()),
        unknown_keys,
        unknown_bytes / 2**30,
    )

    return Census(
        total_bytes=total_bytes,
        total_keys=total_keys,
        unknown_bytes=unknown_bytes,
        unknown_keys=unknown_keys,
        hours=hours,
        cameras=stats,
        newest_epoch=newest_epoch,
        listed_prefix=prefix,
    )


def growth_headroom(census, hours):
    """Bytes the archive is expected to gain over the next ``hours`` hours.

    Summed over cameras - every camera writes during an interval, so the
    fattest camera alone understates it by the camera count. A camera with
    fewer recorded hours than asked for has the shortfall estimated at its own
    mean: a newly added camera fills up faster than its single hour suggests.
    """
    if hours <= 0:
        raise ValueError("hours must be > 0")

    total = 0
    for stat in census.cameras.values():
        sizes = stat.recent_hour_bytes
        if not sizes:
            continue
        known = sizes[-hours:]
        missing = hours - len(known)
        total += sum(known) + missing * (sum(sizes) // len(sizes))
    return total
