import asyncio
import math
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timedelta
from functools import partial
from zoneinfo import ZoneInfo

import boto3
import requests
from requests.auth import HTTPBasicAuth
from botocore.config import Config
from botocore.exceptions import ClientError

import metrics
import mp4_tail


DEFAULT_FRIGATE_URL = "http://localhost:5000"

DEFAULT_RECORDINGS_CHUNK_SECONDS = 3600
DEFAULT_RECORDINGS_OVERLAP_SECONDS = 10
DEFAULT_AFTER_TS_MARGIN_SECONDS = 2.0

DEFAULT_MOTION_THRESHOLD = 1
DEFAULT_MAX_GAP_SECONDS = 40.0
DEFAULT_INTERVAL_CHUNK_SECONDS = 10 * 60
DEFAULT_RECORDINGS_TIMEOUT = 30

DEFAULT_HTTP_CHUNK_SIZE = 1024 * 1024
DEFAULT_PART_SIZE = 5 * 1024 * 1024
# Every multipart part but the last must reach this size, so a smaller part
# size fails complete_multipart_upload() after the whole clip was streamed.
# Keep this and DEFAULT_PART_SIZE consistent; tests rely on the default.
MIN_MULTIPART_PART_SIZE = 5 * 1024 * 1024
DEFAULT_HTTP_TIMEOUT = 300

# Retrying one clip window is shared by every retry chain: a stream cut
# inside its final box, a "no recordings" 400 and an empty 200 all re-fetch
# the same window with the same schedule. Only when every attempt of the
# truncated chain comes back cut is the partial body archived under this key
# suffix. The schedule doubles from the base delay and saturates at
# CLIP_RETRY_MAX_DELAY_SECONDS; the defaults (6 retries, 10+20+40+80+80+80)
# span ~310 s, a budget deliberately sized to ride out a Frigate restart so
# one restart does not seal windows as truncated or close them with a
# marker.
DEFAULT_CLIP_RETRIES = 6
DEFAULT_CLIP_RETRY_BASE_DELAY_SECONDS = 10.0
DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS = 80.0
TRUNCATED_KEY_SUFFIX = "-truncated"

# A window Frigate's database lists but its disk no longer holds answers with
# HTTP 400 and this message. The files are gone, so the window is closed by a
# marker instead of pinning the watermark; the marker carries no media, hence
# no .mp4 extension.
NO_RECORDINGS_MARKER = "No recordings found for the specified time range"
NO_RECORDINGS_BODY_BYTES = 2048
UNAVAILABLE_KEY_SUFFIX = "-no-recordings"
NO_RECORDINGS_KEY_EXTENSION = ""
CLIP_KEY_EXTENSION = ".mp4"

DEFAULT_WATERMARK_LOOKBACK_SECONDS = 24 * 3600
DEFAULT_FIRST_RUN_LOOKBACK_SECONDS = 3 * 24 * 3600

# Raised by the uploader when a clip stream is cut inside its final box; the
# retry path in upload_clip_hybrid owns this error and no other failure can
# reach the caller through it, which is what keeps -truncated.mp4 exclusive.
truncated_clip_error = metrics.ClipTruncatedError

# Raised when Frigate reports no recordings for a window its own scan listed;
# exclusive to the -no-recordings marker for the same reason.
no_recordings_error = metrics.NoRecordingsError

DEFAULT_S3_KEY_TIMEZONE = ZoneInfo("UTC")

DEFAULT_IDLE_SLEEP_SECONDS = 60
DEFAULT_CAMERA_RESTART_BACKOFF_SECONDS = 5.0
DEFAULT_CAMERA_RESTART_BACKOFF_MAX_SECONDS = 300.0

# Startup camera discovery retries: enough to outlast a normal container
# start of Frigate or its reverse proxy, short enough that a wrong address
# still fails fast.
DEFAULT_CAMERA_DISCOVERY_ATTEMPTS = 12
DEFAULT_CAMERA_DISCOVERY_RETRY_SECONDS = 5.0

# Largest exponent whose power of two is still representable as a float;
# beyond it, multiplying by 2**n overflows instead of saturating.
_MAX_BACKOFF_DOUBLINGS = 1023

# S3 timeouts and retry policy, in one place, on the client. Standard retry
# mode retries 5xx / timeouts / throttles with jittered exponential backoff
# capped at 20 s per wait; permanent failures (403, NoSuchBucket, invalid
# credentials) are not retried at all and still abort immediately.
#
# One policy serves two opposite faults, and the same sum bounds both:
#   blackholed S3 (no answer, no RST) costs attempts * read_timeout + backoff
#     = 10 * 8 s + ~70 s = ~150 s, measured ~100-110 s, vs ~310 s before;
#   a fast-failing outage (immediate 5xx) costs only the backoff, so the
#     longest outage that can be ridden out is that same ~70 s.
# A couple of minutes of 5xx cannot be covered together with a 3-minute
# blackhole bound: the outage budget is the backoff sum, and its worst case
# (20 s per retry) is also what the blackhole pays. Raising attempts past this
# point pushes the blackhole past three minutes without a matching gain.
# botocore rewinds a request body only when it can, which is why every Body in
# the upload path is bytes, not a stream - otherwise parts would fail without
# a single retry.
DEFAULT_S3_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_S3_READ_TIMEOUT_SECONDS = 8.0
DEFAULT_S3_TOTAL_MAX_ATTEMPTS = 10
DEFAULT_S3_RETRY_MODE = "standard"


def make_s3_client(
    connect_timeout_seconds=DEFAULT_S3_CONNECT_TIMEOUT_SECONDS,
    read_timeout_seconds=DEFAULT_S3_READ_TIMEOUT_SECONDS,
    total_max_attempts=DEFAULT_S3_TOTAL_MAX_ATTEMPTS,
    retry_mode=DEFAULT_S3_RETRY_MODE,
):
    """Build the S3 client; credentials/region/endpoint stay on boto3 chain.

    Only the time budget is ours: how long one operation may spend on
    attempts and backoff before it raises.
    """
    if connect_timeout_seconds <= 0 or read_timeout_seconds <= 0:
        raise ValueError("timeouts must be > 0")
    if total_max_attempts < 1:
        raise ValueError("total_max_attempts must be >= 1")

    return boto3.client(
        "s3",
        config=Config(
            connect_timeout=connect_timeout_seconds,
            read_timeout=read_timeout_seconds,
            retries={
                "total_max_attempts": int(total_max_attempts),
                "mode": retry_mode,
            },
        ),
    )


def make_frigate_client(
    base_url=DEFAULT_FRIGATE_URL,
    username=None,
    password=None,
):
    """Build the Frigate HTTP client: base URL and auth live here only.

    Credentials default to FRIGATE_USER/FRIGATE_PASSWORD. When no username is
    configured the session stays unauthenticated, which is what a localhost
    deployment exposes.
    """
    if not base_url or not base_url.startswith(("http://", "https://")):
        raise ValueError("base_url must be an http(s) URL")

    username = (
        username
        if username is not None
        else os.environ.get("FRIGATE_USER", "")
    )
    password = (
        password
        if password is not None
        else os.environ.get("FRIGATE_PASSWORD", "")
    )

    session = requests.Session()
    session.headers["Accept"] = "application/json"

    if username:
        session.auth = HTTPBasicAuth(username, password)

    return {
        "session": session,
        "base_url": base_url.rstrip("/"),
    }


def make_frigate_client_factory(
    base_url=DEFAULT_FRIGATE_URL,
    username=None,
    password=None,
):
    """Return a zero-argument factory of Frigate clients sharing one config.

    ``requests.Session`` is documented as not thread-safe, and every camera
    issues its requests from a ``to_thread`` worker, so a session must not be
    shared between cameras. ``async_main`` reads the configuration once, outside
    the business functions, and hands this factory to each supervisor.
    """
    resolved_url = base_url
    resolved_user = (
        username
        if username is not None
        else os.environ.get("FRIGATE_USER", "")
    )
    resolved_password = (
        password
        if password is not None
        else os.environ.get("FRIGATE_PASSWORD", "")
    )

    return lambda: make_frigate_client(
        resolved_url,
        username=resolved_user,
        password=resolved_password,
    )


def frigate_url(frigate_client, path):
    """Join a path onto the client's base URL."""
    return f"{frigate_client['base_url']}{path}"


async def frigate_get(frigate_client, path, **kwargs):
    """GET a small JSON answer from Frigate through the prepared client."""
    session = frigate_client["session"]
    started = time.perf_counter()

    try:
        return await asyncio.to_thread(
            partial(session.get, frigate_url(frigate_client, path), **kwargs)
        )
    finally:
        # Recorded even for a failed call: the wait happened either way, and a
        # Frigate that answers slowly and one that answers with a 500 are two
        # different alerts.
        metrics.FRIGATE_RESPONSE_SECONDS.labels(kind="json").observe(
            time.perf_counter() - started
        )


async def fetch_cameras(frigate_client, timeout=DEFAULT_RECORDINGS_TIMEOUT):
    """Return the camera names Frigate is configured with."""
    response = await frigate_get(
        frigate_client, "/api/config", timeout=timeout
    )
    response.raise_for_status()
    config = response.json()

    if not isinstance(config, dict) or not isinstance(
        config.get("cameras"), dict
    ):
        raise RuntimeError("Frigate returned no camera list")

    return sorted(config["cameras"])


async def fetch_cameras_with_retry(
    frigate_client,
    attempts=DEFAULT_CAMERA_DISCOVERY_ATTEMPTS,
    retry_seconds=DEFAULT_CAMERA_DISCOVERY_RETRY_SECONDS,
    timeout=DEFAULT_RECORDINGS_TIMEOUT,
    sleep=asyncio.sleep,
):
    """Return the camera list, retrying while Frigate finishes starting.

    Discovery is the only Frigate call made before any supervisor exists, so a
    Frigate that is still booting would kill the process here, while the same
    failure one minute later is retried forever by the camera supervisors. The
    budget (12 tries, 5 s apart) covers a normal container or proxy restart;
    after it the last error is raised and startup fails, because an endpoint
    unreachable for a full minute is more likely a configuration problem than
    a transient one.

    ``sleep`` is injectable so tests do not wait out the retry budget.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    if retry_seconds < 0:
        raise ValueError("retry_seconds must be >= 0")

    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return await fetch_cameras(frigate_client, timeout=timeout)
        except Exception as exc:
            last_exc = exc
            print(
                f"Camera discovery failed "
                f"({attempt}/{attempts}): {type(exc).__name__}: {exc}",
                flush=True,
            )
            if attempt < attempts:
                await sleep(retry_seconds)
    raise last_exc


def select_cameras(cameras, include=None, exclude=None):
    """Filter discovered cameras by an include or exclude pattern.

    Both patterns at once is a contradiction, so it is rejected rather than
    resolved by precedence: a silent ordering would decide which cameras get
    archived, which is exactly the thing worth being explicit about. With
    neither set every discovered camera is archived.
    """
    if include is not None and exclude is not None:
        raise ValueError(
            "include and exclude are both set - only one of them is needed"
        )

    cameras = list(cameras)
    if include is not None:
        return [c for c in cameras if include.search(c)]
    if exclude is not None:
        return [c for c in cameras if not exclude.search(c)]
    return cameras


def empty_camera_list_message(discovered, cfg):
    """Explain why there is nothing to archive.

    Frigate returning no cameras and the filter removing all of them look the
    same from the outside but only one of them is ours to fix, so the message
    names the variable and the pattern that produced the empty selection.
    """
    if not discovered:
        return "Frigate returned no cameras"

    pattern = cfg.cameras_include or cfg.cameras_exclude
    name = "CAMERAS_INCLUDE" if cfg.cameras_include else "CAMERAS_EXCLUDE"
    return (
        f"Frigate returned {len(discovered)} cameras, but {name}="
        f"{pattern.pattern!r} left none - there is nothing to archive"
    )


def next_after_ts(
    last_interval_end_ts,
    margin_seconds=DEFAULT_AFTER_TS_MARGIN_SECONDS,
):
    """Return the Frigate query `after` for the next normal pass."""
    if margin_seconds < 0:
        raise ValueError("margin_seconds must be >= 0")
    return float(last_interval_end_ts) + float(margin_seconds)


async def fetch_recordings(
    camera,
    after_ts,
    before_ts,
    frigate_client,
    timeout=DEFAULT_RECORDINGS_TIMEOUT,
):
    """Return the recording-segment list Frigate reports for a window.

    ``after``/``before`` are epoch seconds; Frigate returns every segment whose
    range overlaps the window, which is why callers must filter by the real
    segment start. Transport failures and a non-list payload become a
    RuntimeError so the camera pass aborts rather than scanning a hole.
    """
    params = {
        "after": float(after_ts),
        "before": float(before_ts),
    }

    try:
        response = await frigate_get(
            frigate_client,
            f"/api/{camera}/recordings",
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()
    except (requests.exceptions.RequestException, ValueError) as exc:
        raise RuntimeError(
            f"Failed to fetch Frigate recordings "
            f"{camera} {after_ts}-{before_ts}"
        ) from exc

    if not isinstance(data, list):
        raise RuntimeError(
            f"Frigate returned an unexpected payload for camera {camera}: "
            f"{type(data).__name__}"
        )

    return data


def split_interval_for_archive(
    interval_start,
    interval_end,
    scan_end_ts,
    chunk_seconds=DEFAULT_INTERVAL_CHUNK_SECONDS,
    max_gap_seconds=DEFAULT_MAX_GAP_SECONDS,
    is_last_interval=False,
):
    """Split one detected interval into fixed-size chunks plus an optional tail.

    Full chunks are always returned. A tail shorter than ``chunk_seconds``
    is normally returned too. Only the tail of the last detected interval can
    be mistaken for an event that is still continuing, so only that tail is
    discarded when its end is closer than ``max_gap_seconds`` to the scan
    boundary.
    """
    interval_start = float(interval_start)
    interval_end = float(interval_end)
    scan_end_ts = float(scan_end_ts)

    if interval_end <= interval_start:
        return []
    if chunk_seconds <= 0:
        raise ValueError("chunk_seconds must be > 0")
    if max_gap_seconds < 0:
        raise ValueError("max_gap_seconds must be >= 0")

    chunks = []
    chunk_start = interval_start

    while chunk_start + chunk_seconds <= interval_end:
        chunk_end = chunk_start + chunk_seconds
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    if chunk_start < interval_end:
        tail_end = interval_end

        # The only reason to discard a short tail is that the end of this
        # interval is close enough to the scan boundary that the event may
        # still be continuing and the interval detector may not know its
        # actual end yet.
        if (
            is_last_interval
            and scan_end_ts - tail_end < max_gap_seconds
        ):
            return chunks

        chunks.append((chunk_start, tail_end))

    return chunks


async def find_motion_intervals(
    camera,
    after_ts,
    end_ts,
    frigate_client,
    chunk_seconds=DEFAULT_RECORDINGS_CHUNK_SECONDS,
    overlap_seconds=DEFAULT_RECORDINGS_OVERLAP_SECONDS,
    motion_threshold=DEFAULT_MOTION_THRESHOLD,
    max_gap_seconds=DEFAULT_MAX_GAP_SECONDS,
    interval_chunk_seconds=DEFAULT_INTERVAL_CHUNK_SECONDS,
    recordings_timeout=DEFAULT_RECORDINGS_TIMEOUT,
    min_start_ts=None,
):
    """Find and split motion intervals inside the requested time range.

    ``after_ts`` is the actual lower bound passed to Frigate. During normal
    continuous processing it should be the previous successfully processed
    interval's end plus a small margin (normally +2 seconds).

    ``min_start_ts`` is the actual lower bound for accepted segment starts.
    It is useful because Frigate may return a segment whose range overlaps
    ``after_ts`` while its true start is slightly earlier.

    Every motion interval is split into fixed ``interval_chunk_seconds``
    chunks. A final remainder shorter than that duration is kept unless its
    end is less than ``max_gap_seconds`` from ``end_ts``; that
    remainder is treated as a potentially ongoing tail and postponed.
    """
    after_ts = float(after_ts)
    end_ts = float(end_ts)

    if min_start_ts is None:
        min_start_ts = after_ts
    else:
        min_start_ts = float(min_start_ts)

    if end_ts <= after_ts or end_ts <= min_start_ts:
        return []
    if chunk_seconds <= 0:
        raise ValueError("chunk_seconds must be > 0")
    if overlap_seconds < 0:
        raise ValueError("overlap_seconds must be >= 0")
    if max_gap_seconds < 0:
        raise ValueError("max_gap_seconds must be >= 0")
    if interval_chunk_seconds <= 0:
        raise ValueError("interval_chunk_seconds must be > 0")

    current_time = after_ts
    open_interval_start = None
    last_end_time = None
    raw_intervals = []

    while current_time < end_ts:
        chunk_end = min(current_time + chunk_seconds, end_ts)
        fetch_start = max(after_ts, current_time - overlap_seconds)

        segments = await fetch_recordings(
            camera=camera,
            after_ts=fetch_start,
            before_ts=chunk_end,
            frigate_client=frigate_client,
            timeout=recordings_timeout,
        )

        normalized_segments = []

        for seg in segments:
            try:
                seg_start = float(seg["start_time"])
                seg_end = float(seg["end_time"])
                has_motion = seg.get("motion", 0) >= motion_threshold
            except (KeyError, TypeError, ValueError):
                print(
                    f"[WARNING] Skipped malformed segment: "
                    f"{seg!r}"
                )
                continue

            # Frigate's range query may return a segment whose range only
            # partially overlaps the query. A segment that truly started
            # before our resume boundary belongs to the already processed
            # side and must not be used to reopen an old interval.
            if seg_start < min_start_ts:
                continue

            if seg_start >= end_ts or seg_end <= min_start_ts:
                continue

            effective_end = min(seg_end, end_ts)
            if effective_end <= seg_start:
                continue

            normalized_segments.append(
                (seg_start, effective_end, has_motion)
            )

        normalized_segments.sort(key=lambda item: (item[0], item[1]))

        for seg_start, seg_end, has_motion in normalized_segments:
            if has_motion:
                if open_interval_start is None:
                    open_interval_start = seg_start
                    last_end_time = seg_end
                else:
                    gap = seg_start - last_end_time

                    if gap > max_gap_seconds:
                        raw_intervals.append(
                            (open_interval_start, last_end_time)
                        )
                        open_interval_start = seg_start
                        last_end_time = seg_end
                    else:
                        last_end_time = max(last_end_time, seg_end)

            elif open_interval_start is not None:
                # A motion=0 segment contains no motion, so the current
                # interval ends exactly where that segment starts. No buffer
                # is added because the following segment is explicitly known
                # to contain no motion.
                raw_intervals.append(
                    (open_interval_start, seg_start)
                )
                open_interval_start = None
                last_end_time = None

        current_time += chunk_seconds

    if open_interval_start is not None:
        # The interval reaches the scan boundary without an explicit close.
        # It ends at the last segment that was actually recorded, not at
        # end_ts: the recorder stopped writing, which is exactly what an
        # open interval means here, and time past the last segment holds no
        # footage no matter how far the scan has grown past it. Whether this
        # interval is merely ongoing or truly over is not this function's
        # call - split_interval_for_archive defers the still-ongoing-looking
        # tail via the same max_gap_seconds.
        raw_intervals.append((open_interval_start, last_end_time))

    raw_intervals.sort(key=lambda interval: interval[0])

    final_intervals = []
    for index, (interval_start, interval_end) in enumerate(raw_intervals):
        final_intervals.extend(
            split_interval_for_archive(
                interval_start=interval_start,
                interval_end=interval_end,
                scan_end_ts=end_ts,
                chunk_seconds=interval_chunk_seconds,
                max_gap_seconds=max_gap_seconds,
                is_last_interval=(index == len(raw_intervals) - 1),
            )
        )

    return final_intervals


def make_s3_key(
    camera,
    clip_start_ts,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    suffix="",
    key_prefix="",
    extension=CLIP_KEY_EXTENSION,
):
    """Build the object key of a clip: ``[prefix/]<camera>/YYYY/MM/DD/HH/MM-SS.mp4``.

    The timestamp is the clip start rendered in ``key_timezone``; it is the only
    time component the key stores, which is why the watermark resumes from a
    clip start rather than its end. ``suffix``/``extension`` carry the two
    window-closing variants (``-truncated.mp4``, ``-no-recordings``).
    """
    dt = datetime.fromtimestamp(float(clip_start_ts), tz=key_timezone)

    return (
        f"{key_prefix}{camera}/"
        f"{dt:%Y/%m/%d/%H}/"
        f"{dt:%M-%S}{suffix}{extension}"
    )


def make_unavailable_s3_key(
    camera,
    clip_start_ts,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    key_prefix="",
):
    """The marker key closing a window Frigate can no longer serve.

    The object is empty and carries no .mp4 extension: it is a bookkeeping
    record, and the missing extension keeps it out of any media tooling that
    selects the archive by suffix.
    """
    return make_s3_key(
        camera=camera,
        clip_start_ts=clip_start_ts,
        key_timezone=key_timezone,
        suffix=UNAVAILABLE_KEY_SUFFIX,
        key_prefix=key_prefix,
        extension=NO_RECORDINGS_KEY_EXTENSION,
    )


def parse_s3_clip_start(
    camera,
    key,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    key_prefix="",
):
    """Return the clip start epoch seconds encoded in a key, or None for other keys.

    The exact inverse of :func:`make_s3_key` - keys in any other layout, or with
    an impossible date, parse to None and are ignored by the watermark scan.
    """
    # Every key shape that closes a window counts: the plain clip, the
    # truncated clip, and the no-recordings marker. Kept in sync with the
    # suffixes that write them.
    closed_clip = re.escape(TRUNCATED_KEY_SUFFIX) + re.escape(
        CLIP_KEY_EXTENSION
    )
    closed_marker = re.escape(UNAVAILABLE_KEY_SUFFIX) + re.escape(
        NO_RECORDINGS_KEY_EXTENSION
    )
    pattern = (
        rf"^{re.escape(key_prefix)}{re.escape(camera)}/"
        rf"(\d{{4}})/(\d{{2}})/(\d{{2}})/"
        rf"(\d{{2}})/(\d{{2}})-(\d{{2}})"
        rf"(?:{re.escape(CLIP_KEY_EXTENSION)}|{closed_clip}|{closed_marker})$"
    )
    match = re.match(pattern, key)

    if not match:
        return None

    year, month, day, hour, minute, second = map(int, match.groups())

    try:
        dt = datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            tzinfo=key_timezone,
        )
    except ValueError:
        return None

    return int(dt.timestamp())


def get_last_processed_time(
    camera,
    s3_bucket,
    s3_client=None,
    now_ts=None,
    lookback_seconds=DEFAULT_WATERMARK_LOOKBACK_SECONDS,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    key_prefix="",
):
    """Scan the dated key prefixes of a lookback window for the newest clip start.

    Prefixes are walked day by day (``<prefix><camera>/YYYY/MM/DD/``) in the key
    timezone, because a single full-bucket listing grows with the archive. Returns
    the newest parseable start inside ``[now - lookback_seconds, now]``, or None
    when the window holds none - the cold-resume case the caller resolves with
    its first-run lookback. A listing error is raised: guessing a watermark is
    how a hole gets skipped over.
    """
    if s3_client is None:
        s3_client = make_s3_client()
    if lookback_seconds < 0:
        raise ValueError("lookback_seconds must be >= 0")

    if now_ts is None:
        now_ts = int(time.time())
    else:
        now_ts = int(now_ts)

    cutoff_ts = now_ts - int(lookback_seconds)

    cutoff_local = datetime.fromtimestamp(cutoff_ts, tz=key_timezone)
    now_local = datetime.fromtimestamp(now_ts, tz=key_timezone)

    last_start_ts = None
    current_date = cutoff_local.date()
    end_date = now_local.date()

    paginator = s3_client.get_paginator("list_objects_v2")

    while current_date <= end_date:
        prefix = f"{key_prefix}{camera}/{current_date:%Y/%m/%d}/"

        try:
            pages = paginator.paginate(
                Bucket=s3_bucket,
                Prefix=prefix,
            )

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")

                    start_ts = parse_s3_clip_start(
                        camera=camera,
                        key=key,
                        key_timezone=key_timezone,
                        key_prefix=key_prefix,
                    )
                    if start_ts is None:
                        continue

                    if not (cutoff_ts <= start_ts <= now_ts):
                        continue

                    if last_start_ts is None or start_ts > last_start_ts:
                        last_start_ts = start_ts

        except ClientError as exc:
            raise RuntimeError(
                f"Failed to list S3 prefix {prefix}"
            ) from exc

        current_date += timedelta(days=1)

    return last_start_ts


def s3_key_exists(
    s3_bucket,
    s3_key,
    s3_client,
):
    """Return whether a complete object already exists at this exact key."""
    try:
        s3_client.head_object(
            Bucket=s3_bucket,
            Key=s3_key,
        )
        return True
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = str(error.get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )

        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False

        raise RuntimeError(
            f"Failed to check whether S3 object {s3_key} exists"
        ) from exc


def _check_cancelled(cancel_event, camera, s3_key):
    """Stop a worker thread that was abandoned by a cancelled to_thread().

    A thread cannot be killed and ``to_thread`` only detaches the await, so the
    only way cancellation ends the physical work is to check between operations.
    Raising here lets the multipart cleanup below run as it does for any other
    failure. The work left behind is always resumable: the watermark does not
    advance over an abandoned clip, so the camera re-requests it after a restart.
    """
    if cancel_event is None or not cancel_event.is_set():
        return
    metrics.record_failure(camera, "cancelled")
    raise asyncio.CancelledError(
        f"{camera}: upload of {s3_key} stopped by task cancellation"
    )


def _is_no_recordings(exc):
    """Is this the 400 that means the window's recordings no longer exist?

    Only the clip endpoint's own message counts, matched on a small prefix of
    the error body. The body of an HTTP error is bounded and never feeds the
    upload stream, so reading it cannot cost clip memory; a non-JSON body (an
    nginx HTML page, say) simply fails the substring test.
    """
    response = getattr(exc, "response", None)
    if response is None or response.status_code != 400:
        return False
    try:
        body = next(
            response.iter_content(chunk_size=NO_RECORDINGS_BODY_BYTES), b""
        )
    except Exception:
        return False
    return NO_RECORDINGS_MARKER in body.decode("utf-8", errors="replace")


def _upload_clip_hybrid_sync(
    camera,
    start_ts,
    end_ts,
    frigate_client,
    s3_bucket,
    s3_client,
    key_timezone,
    part_size,
    http_chunk_size,
    http_timeout,
    cancel_event=None,
    key_suffix="",
    allow_truncated=False,
    key_prefix="",
):
    """Blocking part of upload_clip_hybrid, run in a worker thread.

    ``cancel_event`` is set by the awaiting task when it is cancelled; the
    thread then stops at the next boundary, aborts the partial multipart
    upload, and raises CancelledError instead of finishing the clip.

    ``key_suffix`` is appended to the S3 key (before .mp4) so a retry can
    archive under a different name. A window counts as archived when any
    object that closes it exists - the plain clip, the truncated clip, or the
    no-recordings marker: each is the last word Frigate will ever give for
    that window, so re-fetching it every pass would repeat a download that
    cannot improve.

    ``allow_truncated`` skips the completeness check for the final save pass,
    whose whole purpose is archiving a body known to be cut off.
    """
    s3_key = make_s3_key(
        camera=camera,
        clip_start_ts=start_ts,
        key_timezone=key_timezone,
        suffix=key_suffix,
        key_prefix=key_prefix,
    )

    # A window counts as closed when any object that closes it exists: the
    # clip itself, its truncated variant, or the no-recordings marker. Each is
    # the last word Frigate will ever give for the window, so re-fetching it
    # every pass would repeat a download that cannot improve.
    closed_keys = [
        s3_key,
        make_s3_key(
            camera=camera,
            clip_start_ts=start_ts,
            key_timezone=key_timezone,
            suffix=TRUNCATED_KEY_SUFFIX,
            key_prefix=key_prefix,
        ),
        make_unavailable_s3_key(
            camera=camera,
            clip_start_ts=start_ts,
            key_timezone=key_timezone,
            key_prefix=key_prefix,
        ),
    ]
    if any(
        s3_key_exists(
            s3_bucket=s3_bucket,
            s3_key=closed_key,
            s3_client=s3_client,
        )
        for closed_key in closed_keys
    ):
        print(f"[EXISTS] Already uploaded: {s3_key}")
        metrics.CLIPS_SKIPPED_TOTAL.labels(camera=camera).inc()
        return False

    # Frigate declares these path parameters as float (and its own UI sends
    # raw JS numbers), so any plain decimal parses. Integer seconds are used
    # deliberately: Frigate's recording rows carry whole-second boundaries,
    # floor/ceil widens the window outward so no segment the scan saw can fall
    # outside it, and an integer never renders in scientific notation (a "%g"
    # epoch collapses to 6 significant digits: start and end merge, and the
    # value shifts by up to 500 s - Frigate then cheerfully exports the wrong
    # window, it does not reject the URL).
    clip_url = frigate_url(
        frigate_client,
        f"/api/{camera}/start/{math.floor(start_ts)}"
        f"/end/{math.ceil(end_ts)}/clip.mp4",
    )

    # Frigate renders a clip export before it sends anything, so time-to-first
    # byte is the number that shows Frigate struggling rather than the network.
    started = time.perf_counter()
    with frigate_client["session"].get(
        clip_url,
        stream=True,
        timeout=http_timeout,
    ) as response:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            if _is_no_recordings(exc):
                metrics.record_failure(camera, "frigate_no_recordings")
                raise metrics.NoRecordingsError(
                    f"Frigate lost the recordings for window {camera} "
                    f"{start_ts}-{end_ts}"
                ) from exc
            raise
        metrics.FRIGATE_RESPONSE_SECONDS.labels(kind="clip_ttfb").observe(
            time.perf_counter() - started
        )

        stream = response.iter_content(chunk_size=http_chunk_size)

        buffer = bytearray()
        tail = bytearray()
        tail_limit = max(2 * mp4_tail.TAIL_WINDOW_BYTES, http_chunk_size)
        total_bytes = 0
        stream_exhausted = False

        def absorb(chunk):
            """Feed one stream chunk into the part buffer and the tail."""
            nonlocal total_bytes
            buffer.extend(chunk)
            tail.extend(chunk)
            if len(tail) > tail_limit:
                del tail[:len(tail) - tail_limit]
            total_bytes += len(chunk)

        # First fill one multipart-sized buffer. Small clips use PUT;
        # larger clips switch to multipart without a temporary file.
        for chunk in stream:
            _check_cancelled(cancel_event, camera, s3_key)
            if not chunk:
                continue

            absorb(chunk)

            if len(buffer) >= part_size:
                break
        else:
            stream_exhausted = True

        if total_bytes == 0:
            metrics.record_failure(camera, "frigate_empty_clip")
            raise metrics.EmptyClipError(
                f"Frigate returned an empty clip {camera} {start_ts}-{end_ts}"
            )

        if stream_exhausted and not allow_truncated:
            complete, reason = mp4_tail.stream_is_complete(tail)
            if not complete:
                metrics.record_failure(
                    camera, "frigate_clip_truncated"
                )
                raise metrics.ClipTruncatedError(
                    f"Frigate returned a truncated clip {camera} "
                    f"{start_ts}-{end_ts} ({total_bytes} B): {reason}"
                )

        if stream_exhausted and total_bytes < part_size:
            _check_cancelled(cancel_event, camera, s3_key)
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=bytes(buffer),
                ContentType="video/mp4",
            )
            metrics.CLIPS_UPLOADED_TOTAL.labels(camera=camera).inc()
            metrics.CLIP_BYTES_UPLOADED_TOTAL.labels(camera=camera).inc(
                total_bytes
            )
            print(
                f"[PUT] Uploaded: {s3_key} "
                f"({total_bytes / 1024 / 1024:.2f} MB)"
            )
            return True

        mpu = s3_client.create_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
            ContentType="video/mp4",
        )

        upload_id = mpu["UploadId"]
        parts = []
        part_number = 1

        try:
            while len(buffer) >= part_size:
                _check_cancelled(cancel_event, camera, s3_key)
                part_data = bytes(buffer[:part_size])
                del buffer[:part_size]

                result = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=part_data,
                )
                parts.append(
                    {
                        "PartNumber": part_number,
                        "ETag": result["ETag"],
                    }
                )
                part_number += 1

            for chunk in stream:
                _check_cancelled(cancel_event, camera, s3_key)
                if not chunk:
                    continue

                absorb(chunk)

                while len(buffer) >= part_size:
                    part_data = bytes(buffer[:part_size])
                    del buffer[:part_size]

                    result = s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=part_data,
                    )
                    parts.append(
                        {
                            "PartNumber": part_number,
                            "ETag": result["ETag"],
                        }
                    )
                    part_number += 1

            _check_cancelled(cancel_event, camera, s3_key)

            if not allow_truncated:
                complete, reason = mp4_tail.stream_is_complete(tail)
                if not complete:
                    metrics.record_failure(
                        camera, "frigate_clip_truncated"
                    )
                    raise metrics.ClipTruncatedError(
                        f"Frigate returned a truncated clip {camera} "
                        f"{start_ts}-{end_ts} ({total_bytes} B): {reason}"
                    )

            if buffer:
                result = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=bytes(buffer),
                )
                parts.append(
                    {
                        "PartNumber": part_number,
                        "ETag": result["ETag"],
                    }
                )

            s3_client.complete_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            metrics.CLIPS_UPLOADED_TOTAL.labels(camera=camera).inc()
            metrics.CLIP_BYTES_UPLOADED_TOTAL.labels(camera=camera).inc(
                total_bytes
            )
            print(
                f"[MPU] Uploaded: {s3_key} "
                f"({total_bytes / 1024 / 1024:.2f} MB)"
            )
            return True

        except BaseException:
            # CancelledError is a BaseException: a plain ``except Exception``
            # here would leave a half-uploaded multipart object in the bucket,
            # billed as storage and invisible to the next run.
            try:
                s3_client.abort_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                )
            except Exception:
                pass
            raise


def _write_unavailable_marker_sync(
    camera,
    start_ts,
    s3_bucket,
    s3_client,
    key_timezone,
    key_prefix="",
):
    """Close a window Frigate can no longer serve with an empty marker object.

    Run in a worker thread like the uploader. The object is deliberately not a
    clip: zero bytes and no .mp4 extension, so nothing mistakes it for media.
    Whether it is written or skipped, the window ends up closed, which is what
    lets the watermark advance instead of pinning on a window that can never
    be fetched again.
    """
    marker_key = make_unavailable_s3_key(
        camera=camera,
        clip_start_ts=start_ts,
        key_timezone=key_timezone,
        key_prefix=key_prefix,
    )

    if s3_key_exists(
        s3_bucket=s3_bucket,
        s3_key=marker_key,
        s3_client=s3_client,
    ):
        print(f"[EXISTS] Already closed by marker: {marker_key}")
        metrics.CLIPS_SKIPPED_TOTAL.labels(camera=camera).inc()
        return False

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=marker_key,
        Body=b"",
        ContentType="application/x-empty",
    )
    print(
        f"[NO RECORDINGS] Window {camera} {start_ts} closed by marker: "
        f"{marker_key}"
    )
    return True


async def upload_clip_hybrid(
    camera,
    start_ts,
    end_ts,
    frigate_client,
    s3_bucket=None,
    s3_client=None,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    part_size=DEFAULT_PART_SIZE,
    http_chunk_size=DEFAULT_HTTP_CHUNK_SIZE,
    http_timeout=DEFAULT_HTTP_TIMEOUT,
    clip_retries=DEFAULT_CLIP_RETRIES,
    clip_retry_max_delay=DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS,
    key_prefix="",
):
    """Stream one clip from Frigate into S3.

    Frigate and S3 failures are raised to the caller instead of being
    reported as a skipped clip: continuing past a failure would publish a
    later clip ahead of an earlier one and let the watermark advance over a
    hole in the archive.

    A truncated stream (its final box does not reach EOF) is retried in
    place. If every
    attempt - and only this failure - comes back truncated, the received
    body is archived under a ``-truncated.mp4`` key: the footage is real and
    partial beats losing it to a permanent Frigate fault. Any other error is
    raised unchanged, so it can never produce a truncated key.

    A window whose recordings no longer exist is retried the same way, and
    Frigate says that in two ways: its "no recordings" 400 (no rows), and
    HTTP 200 with an empty body (rows exist but ffmpeg could not open the
    files - it dies before emitting the first packet, and the 200 headers are
    already on the wire). Both mean the same thing one layer apart, so when
    every attempt repeats either answer an empty ``-no-recordings`` marker
    closes the window instead of pinning the watermark on footage that can
    never be exported again. No other error produces a marker.

    Returns ``(is_new, was_truncated)``.
    """
    if s3_bucket is None:
        raise ValueError("s3_bucket must be set")
    if s3_client is None:
        s3_client = make_s3_client()
    if part_size < MIN_MULTIPART_PART_SIZE:
        raise ValueError(
            f"part_size must be at least "
            f"{MIN_MULTIPART_PART_SIZE} (the S3 minimum for multipart parts)"
        )
    if http_chunk_size <= 0:
        raise ValueError("http_chunk_size must be > 0")
    if clip_retries < 0:
        raise ValueError("clip_retries must be >= 0")
    if clip_retry_max_delay < 0:
        raise ValueError("clip_retry_max_delay must be >= 0")

    start_ts = float(start_ts)
    end_ts = float(end_ts)

    if end_ts <= start_ts:
        raise ValueError(
            f"Invalid clip range for {camera}: "
            f"{start_ts}-{end_ts}"
        )

    # Faults seen by this window's chain, by family: a closure object (the
    # truncated save, the marker) requires the whole chain to be one family.
    seen_faults = set()

    for attempt in range(int(clip_retries) + 1):
        # to_thread cancellation only detaches: the worker keeps running unless it
        # is told to stop, so the flag is the one channel that reaches it.
        cancel_event = threading.Event()
        try:
            return await asyncio.to_thread(
                _upload_clip_hybrid_sync,
                camera,
                start_ts,
                end_ts,
                frigate_client,
                s3_bucket,
                s3_client,
                key_timezone,
                part_size,
                http_chunk_size,
                http_timeout,
                cancel_event,
                "",
                False,
                key_prefix,
            ), False
        except asyncio.CancelledError:
            cancel_event.set()
            raise
        except (metrics.NoRecordingsError, metrics.EmptyClipError):
            # Either answer means the window's recordings no longer exist:
            # the 400 is an empty database query, the empty 200 is ffmpeg
            # failing to open files the database still lists. Both are
            # retried anyway - the scan listed segments for this window
            # moments ago, which covers the race where Frigate deletes them
            # mid-export - but once every attempt repeats an
            # unavailable-window answer, no retry can bring the files back,
            # so the window is closed by a marker.
            seen_faults.add("unavailable")
            if attempt >= int(clip_retries):
                # A chain that also saw truncated bodies proves recordings
                # existed and were partially exportable: which fault is
                # permanent is unknown, so nothing is sealed and the window
                # is retried on a later pass with a fresh chain.
                if seen_faults != {"unavailable"}:
                    raise
                is_new = await asyncio.to_thread(
                    _write_unavailable_marker_sync,
                    camera,
                    start_ts,
                    s3_bucket,
                    s3_client,
                    key_timezone,
                    key_prefix,
                )
                if is_new:
                    metrics.CLIPS_UNAVAILABLE_UPLOADED_TOTAL.labels(
                        camera=camera
                    ).inc()
                return is_new, False
            delay = clip_retry_delay(attempt, max_delay=clip_retry_max_delay)
            if delay > 0:
                await asyncio.sleep(delay)
        except metrics.ClipTruncatedError:
            if clip_retries <= 0:
                raise
            seen_faults.add("truncated")
            if attempt >= int(clip_retries):
                # Only an all-truncated chain has proven the export itself is
                # broken; a chain that also answered unavailable-window must
                # not archive a body that may no longer exist on disk.
                if seen_faults != {"truncated"}:
                    raise
                break
            delay = clip_retry_delay(attempt, max_delay=clip_retry_max_delay)
            if delay > 0:
                await asyncio.sleep(delay)

    # Every attempt came back truncated: archive what arrived under a
    # distinguishable key so the hole is visible and the watermark advances.
    is_new = await asyncio.to_thread(
        _upload_clip_hybrid_sync,
        camera,
        start_ts,
        end_ts,
        frigate_client,
        s3_bucket,
        s3_client,
        key_timezone,
        part_size,
        http_chunk_size,
        http_timeout,
        None,
        TRUNCATED_KEY_SUFFIX,
        True,
        key_prefix,
    )
    if is_new:
        metrics.CLIPS_TRUNCATED_UPLOADED_TOTAL.labels(
            camera=camera
        ).inc()
    return is_new, True


async def upload_intervals(
    intervals,
    camera,
    frigate_client,
    s3_bucket=None,
    s3_client=None,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    part_size=DEFAULT_PART_SIZE,
    http_chunk_size=DEFAULT_HTTP_CHUNK_SIZE,
    http_timeout=DEFAULT_HTTP_TIMEOUT,
    clip_retries=DEFAULT_CLIP_RETRIES,
    clip_retry_max_delay=DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS,
    key_prefix="",
):
    """Upload intervals strictly in chronological order, failing fast.

    Returns ``(uploaded, skipped, last_end_ts, truncated)``. An upload
    failure is raised rather than counted: the watermark is derived from the
    newest clip in the bucket, so publishing a later clip over a failed
    earlier one would hide the hole forever. ``truncated`` counts clips that
    were archived after every attempt came back truncated.
    """
    if s3_bucket is None:
        raise ValueError("s3_bucket must be set")

    uploaded = 0
    skipped = 0
    truncated = 0
    last_end_ts = None

    for interval in sorted(
        intervals, key=lambda interval: float(interval[0])
    ):
        interval_start, interval_end = interval[0], interval[1]
        is_new, was_truncated = await upload_clip_hybrid(
            camera=camera,
            start_ts=interval_start,
            end_ts=interval_end,
            frigate_client=frigate_client,
            s3_bucket=s3_bucket,
            s3_client=s3_client,
            key_timezone=key_timezone,
            part_size=part_size,
            http_chunk_size=http_chunk_size,
            http_timeout=http_timeout,
            clip_retries=clip_retries,
            clip_retry_max_delay=clip_retry_max_delay,
            key_prefix=key_prefix,
        )

        if is_new:
            uploaded += 1
        else:
            skipped += 1
        if was_truncated:
            truncated += 1

        last_end_ts = float(interval_end)

    return uploaded, skipped, last_end_ts, truncated


async def resolve_camera_watermark(
    camera,
    s3_bucket,
    s3_client,
    now_ts,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    watermark_lookback_seconds=DEFAULT_WATERMARK_LOOKBACK_SECONDS,
    first_run_lookback_seconds=DEFAULT_FIRST_RUN_LOOKBACK_SECONDS,
    key_prefix="",
):
    """Return the timestamp to resume scanning a camera from."""
    last_start = await asyncio.to_thread(
        get_last_processed_time,
        camera=camera,
        s3_bucket=s3_bucket,
        s3_client=s3_client,
        now_ts=now_ts,
        lookback_seconds=watermark_lookback_seconds,
        key_timezone=key_timezone,
        key_prefix=key_prefix,
    )

    if last_start is not None:
        print(f"[{camera}] Resuming from timestamp: {last_start}")
        return float(last_start)

    start_time = float(now_ts) - float(first_run_lookback_seconds)
    print(
        f"[{camera}] No clips found in S3. "
        f"Starting at {start_time} "
        f"(the last "
        f"{int(first_run_lookback_seconds) // 3600} h)."
    )
    return start_time


async def run_camera(
    camera,
    frigate_client,
    s3_bucket,
    s3_client=None,
    key_timezone=DEFAULT_S3_KEY_TIMEZONE,
    watermark_lookback_seconds=DEFAULT_WATERMARK_LOOKBACK_SECONDS,
    first_run_lookback_seconds=DEFAULT_FIRST_RUN_LOOKBACK_SECONDS,
    idle_sleep_seconds=DEFAULT_IDLE_SLEEP_SECONDS,
    part_size=DEFAULT_PART_SIZE,
    http_chunk_size=DEFAULT_HTTP_CHUNK_SIZE,
    http_timeout=DEFAULT_HTTP_TIMEOUT,
    now_fn=time.time,
    on_pass_ok=None,
    clip_retries=DEFAULT_CLIP_RETRIES,
    clip_retry_max_delay=DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS,
    max_gap_seconds=DEFAULT_MAX_GAP_SECONDS,
    key_prefix="",
):
    """Archive one camera forever: watermark, scan, upload, rescan, wait.

    ``on_pass_ok`` is called after every pass that reached Frigate and S3
    without an error, which is how the supervisor knows a restart was followed
    by recovery rather than by another crash.
    """
    now_fn = now_fn or time.time
    on_pass_ok = on_pass_ok or (lambda: None)

    watermark = await resolve_camera_watermark(
        camera=camera,
        s3_bucket=s3_bucket,
        s3_client=s3_client,
        now_ts=int(now_fn()),
        key_timezone=key_timezone,
        watermark_lookback_seconds=watermark_lookback_seconds,
        first_run_lookback_seconds=first_run_lookback_seconds,
        key_prefix=key_prefix,
    )
    # Set here rather than by the supervisor: a task wedged in the watermark
    # lookup has not started working, and must not report itself as working.
    metrics.CAMERA_TASK_RUNNING.labels(camera=camera).set(1)

    while True:
        now_ts = int(now_fn())

        # The watermark is the logical coverage boundary: everything before
        # it is archived. Frigate's query may start a bit later (overlap
        # margin), but min_start_ts must stay exactly on the boundary, or a
        # segment straddling it gets dropped by the segment filter and its
        # clip is lost forever.
        #
        # In-process the boundary is an interval *end*; after a restart it is
        # rebuilt as the *start* of the newest clip, because the clip's end is
        # not part of its key. That regression is safe - see
        # resolve_camera_watermark.
        intervals = await find_motion_intervals(
            camera=camera,
            after_ts=next_after_ts(watermark),
            end_ts=now_ts,
            frigate_client=frigate_client,
            chunk_seconds=DEFAULT_RECORDINGS_CHUNK_SECONDS,
            overlap_seconds=DEFAULT_RECORDINGS_OVERLAP_SECONDS,
            motion_threshold=DEFAULT_MOTION_THRESHOLD,
            max_gap_seconds=max_gap_seconds,
            interval_chunk_seconds=DEFAULT_INTERVAL_CHUNK_SECONDS,
            recordings_timeout=DEFAULT_RECORDINGS_TIMEOUT,
            min_start_ts=watermark,
        )
        metrics.CAMERA_MOTION_INTERVALS_FOUND.labels(camera=camera).set(
            len(intervals)
        )

        if not intervals:
            print(
                f"[{camera}] No intervals. "
                f"Sleeping {int(idle_sleep_seconds)} s."
            )
            on_pass_ok()
            await asyncio.sleep(idle_sleep_seconds)
            continue

        print(f"[{camera}] Intervals found: {len(intervals)}")

        uploaded, skipped, last_end_ts, truncated = await upload_intervals(
            intervals=intervals,
            camera=camera,
            frigate_client=frigate_client,
            s3_bucket=s3_bucket,
            s3_client=s3_client,
            key_timezone=key_timezone,
            part_size=part_size,
            http_chunk_size=http_chunk_size,
            http_timeout=http_timeout,
            clip_retries=clip_retries,
            clip_retry_max_delay=clip_retry_max_delay,
            key_prefix=key_prefix,
        )

        if truncated:
            print(
                f"[{camera}] WARNING: {truncated} clip(s) saved "
                f"as truncated - Frigate cut the stream "
                f"{clip_retries + 1} times in a row"
            )

        print(
            f"[{camera}] Uploaded: {uploaded}, "
            f"already present: {skipped}"
        )

        # The whole pass is archived in order, so everything up to its end is
        # in S3. The watermark stays a pure boundary (no margin): the margin
        # is added per query in after_ts only.
        watermark = last_end_ts
        on_pass_ok()


def camera_restart_delay(failures,
                         base=DEFAULT_CAMERA_RESTART_BACKOFF_SECONDS,
                         maximum=DEFAULT_CAMERA_RESTART_BACKOFF_MAX_SECONDS):
    """Seconds to wait before restarting a camera task after ``failures`` crashes."""
    if failures <= 0:
        raise ValueError("failures must be > 0")
    if base <= 0:
        raise ValueError("base must be > 0")
    # A long outage drives failures into the thousands, and float(2**1024)
    # raises OverflowError, which would kill the supervisor. 2**1023 is the
    # largest power of two a float can hold, and the cap makes every larger
    # exponent equivalent to it.
    exponent = min(failures - 1, _MAX_BACKOFF_DOUBLINGS)
    return min(float(base) * (2 ** exponent), float(maximum))


def clip_retry_delay(attempt,
                     base=DEFAULT_CLIP_RETRY_BASE_DELAY_SECONDS,
                     max_delay=DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS):
    """Seconds to wait after the ``attempt``-th failed fetch of one window.

    Doubles from ``base`` and saturates at ``max_delay``. The exponent is
    clamped like camera_restart_delay does: a huge attempt count must not
    build an int whose float conversion overflows.
    """
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    if base <= 0:
        raise ValueError("base must be > 0")
    if max_delay < 0:
        raise ValueError("max_delay must be >= 0")
    exponent = min(attempt, _MAX_BACKOFF_DOUBLINGS)
    return min(float(base) * (2 ** exponent), float(max_delay))


async def supervise_camera(
    camera,
    runner,
    backoff_seconds=DEFAULT_CAMERA_RESTART_BACKOFF_SECONDS,
    backoff_max_seconds=DEFAULT_CAMERA_RESTART_BACKOFF_MAX_SECONDS,
    sleep=asyncio.sleep,
):
    """Keep one camera archiving by restarting its task when the task dies.

    ``runner(on_pass_ok=...)`` returns a fresh awaitable per attempt; a task
    object cannot be awaited twice. The restart clock belongs to the supervisor,
    not to the task: it resets on a pass that reached Frigate and S3, not on the
    task merely surviving, so a camera that crashes every minute backs off
    instead of hammering Frigate, while a camera that recovered an hour ago
    restarts again at the base delay.

    Cancelling the supervisor cancels the camera task it is running.
    """
    metrics.CAMERA_TASK_RUNNING.labels(camera=camera).set(0)
    metrics.CAMERA_CONSECUTIVE_FAILURES.labels(camera=camera).set(0)
    state = {"failures": 0}

    def mark_healthy():
        state["failures"] = 0
        metrics.CAMERA_CONSECUTIVE_FAILURES.labels(camera=camera).set(0)

    while True:
        task = None
        try:
            task = asyncio.ensure_future(runner(on_pass_ok=mark_healthy))
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            raise
        except Exception as exc:
            outcome = metrics.record_error(camera, exc)
        else:
            # run_camera is meant to run forever; returning is our own bug
            # or a permanent stop, and it must not look like health.
            outcome = metrics.record_failure(camera, "task_returned")
        finally:
            # Backoff is not working time: the gauge tracks the camera task,
            # not the supervisor keeping it alive.
            metrics.CAMERA_TASK_RUNNING.labels(camera=camera).set(0)

        metrics.CAMERA_RESTARTS_TOTAL.labels(camera=camera).inc()
        state["failures"] += 1
        failures = state["failures"]
        metrics.CAMERA_CONSECUTIVE_FAILURES.labels(camera=camera).set(
            failures
        )
        delay = camera_restart_delay(
            failures, backoff_seconds, backoff_max_seconds
        )
        print(
            f"[{camera}] Task stopped ({outcome}). "
            f"Restarting in {delay:.0f} s (in a row: {failures})",
            flush=True,
        )
        await sleep(delay)


async def async_main(config, now_fn=time.time):
    """Run every camera, taking all configuration from ``config``.

    The config object is built outside, by :func:`config.load_config`, so that
    no business function reads the environment itself.
    """
    cfg = config
    s3_bucket = cfg.s3_bucket
    key_timezone = cfg.s3_key_timezone

    bound_port = metrics.start_metrics_server(
        port=metrics.metrics_port_from_env(),
        bind=metrics.metrics_bind_from_env(),
    )
    print(f"Metrics: http://:{bound_port}/metrics")

    try:
        return await _run_cameras(cfg, s3_bucket, key_timezone, now_fn)
    finally:
        metrics.shutdown_metrics_server()


async def _run_cameras(cfg, s3_bucket, key_timezone, now_fn):
    frigate_client_factory = make_frigate_client_factory(
        cfg.frigate_url,
        username=cfg.frigate_user,
        password=cfg.frigate_password,
    )
    frigate_client = frigate_client_factory()
    s3_client = make_s3_client()
    print(
        f"S3: bucket={s3_bucket} "
        f"prefix={cfg.s3_prefix or '(none)'} "
        f"endpoint={s3_client.meta.endpoint_url} "
        f"region={s3_client.meta.region_name}"
    )

    cameras = await fetch_cameras_with_retry(frigate_client)
    discovered = list(cameras)
    cameras = select_cameras(
        cameras, include=cfg.cameras_include, exclude=cfg.cameras_exclude
    )

    if not cameras:
        print(empty_camera_list_message(discovered, cfg))
        return

    print(f"Cameras: {', '.join(cameras)}")

    def start_camera(camera):
        return asyncio.create_task(
            supervise_camera(
                camera,
                lambda on_pass_ok: run_camera(
                    camera=camera,
                    frigate_client=frigate_client_factory(),
                    s3_bucket=s3_bucket,
                    s3_client=s3_client,
                    key_timezone=key_timezone,
                    now_fn=now_fn,
                    on_pass_ok=on_pass_ok,
                    watermark_lookback_seconds=cfg.watermark_lookback_seconds,
                    first_run_lookback_seconds=cfg.first_run_lookback_seconds,
                    idle_sleep_seconds=cfg.idle_sleep_seconds,
                    part_size=cfg.s3_part_size,
                    http_timeout=cfg.http_timeout,
                    clip_retries=cfg.clip_retries,
                    clip_retry_max_delay=(
                        cfg.clip_retry_max_delay_seconds
                    ),
                    max_gap_seconds=cfg.max_gap_seconds,
                    key_prefix=cfg.s3_prefix,
                ),
                backoff_seconds=cfg.camera_restart_backoff_seconds,
                backoff_max_seconds=cfg.camera_restart_backoff_max_seconds,
            ),
            name=f"supervisor:{camera}",
        )

    supervisors = {camera: start_camera(camera) for camera in cameras}

    try:
        # A camera whose task dies is restarted by its own supervisor, so a
        # broken camera costs only its own freshness: its watermark stops
        # advancing (no hole is skipped over), camera_restarts_total climbs,
        # and the other cameras keep archiving. The process exits only when a
        # supervisor itself dies, which is a bug in the supervisor.
        await asyncio.wait(
            supervisors.values(), return_when=asyncio.FIRST_EXCEPTION
        )
    except asyncio.CancelledError:
        for task in supervisors.values():
            task.cancel()
        await asyncio.gather(*supervisors.values(), return_exceptions=True)
        raise

    failures = []
    for camera, task in supervisors.items():
        if not task.done() or task.cancelled():
            continue
        error = task.exception()
        if error is not None:
            failures.append((camera, error))
        else:
            failures.append((camera, RuntimeError("supervisor exited")))

    # Everything else keeps running while we report, so a crash of one
    # supervisor does not strand the rest mid-upload.
    for task in supervisors.values():
        task.cancel()
    await asyncio.gather(*supervisors.values(), return_exceptions=True)

    if failures:
        camera, error = failures[0]
        raise RuntimeError(
            f"The supervisor of camera {camera} exited - "
            f"this is a supervisor failure, not a camera failure"
        ) from error


async def run_until_signalled(coro):
    """Await ``coro``; SIGINT/SIGTERM cancel it instead of interrupting Python.

    Without handlers, Ctrl-C raises KeyboardInterrupt at whatever interpreter
    line is executing and asyncio.run re-raises it with a traceback, and a
    SIGTERM from docker or systemd kills the process mid-upload. Both signals
    instead cancel this task here, which routes the stop through the same
    cooperative cancellation the supervisors already handle: camera tasks
    finish their current await point, no traceback. A second signal restores
    the default behavior, so a wedged shutdown can still be killed outright.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()

    def stop(sig_name):
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        print(f"Received {sig_name}, shutting down...", flush=True)
        task.cancel()

    handled = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, partial(stop, sig.name))
        except NotImplementedError:
            # the event loop cannot watch signals; KeyboardInterrupt stays
            # the stop mechanism on such platforms
            continue
        handled.append(sig)
    try:
        return await coro
    finally:
        for sig in handled:
            loop.remove_signal_handler(sig)


def main():
    """Load the environment configuration and archive until interrupted."""
    # Imported here rather than at module scope: config reads the DEFAULT_*
    # constants from this module, so importing it at the top would be circular.
    from config import load_config

    config = load_config()
    try:
        asyncio.run(run_until_signalled(async_main(config=config)))
    except asyncio.CancelledError:
        # A signal requested the stop and the cancellation path completed:
        # an expected exit, not an error to report.
        sys.exit(0)
    except KeyboardInterrupt:
        # A signal that arrived outside the loop (config loading, teardown),
        # where the default handler still applies: also not a crash.
        sys.exit(130)


if __name__ == "__main__":
    main()
