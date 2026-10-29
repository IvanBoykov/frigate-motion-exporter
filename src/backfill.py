"""One-shot backfill: archive an explicitly bounded historical window.

The archiver's resume path is a watermark: it archives forward from the newest
clip in the bucket, and a cold start never looks deeper than
``FIRST_RUN_LOOKBACK_SECONDS``. Downtime longer than that lookback leaves
footage Frigate still holds that the loop will never fetch - documented
behavior, because a free-running process guessing "older than my window" has
no bound worth trusting. This module is the bound the operator supplies: one
pass per camera over an explicit [after, before), then the process exits.

What the mode deliberately does not touch:

- The watermark. A backfill writes the same keys the normal loop writes, so
  the resume position keeps its single source of truth - S3 itself. The
  window may sit below the watermark (the gap case), across it, or above it;
  none of them move the live loop's boundary.
- Retention. The pass ignores the upload age cap: footage the operator named
  is not "doomed by the day threshold", which is what the cap protects
  against. If it is older than RETENTION_KEEP_DAYS, the next prune pass
  deletes it again - the documented interaction: a backfill only durably
  fills a gap the retention thresholds would keep.
- The deferred-tail rule. The live loop postpones a tail close to the scan
  boundary because the event may still be running. Inside a historical
  window every segment is closed, so a fully-past window is scanned as a
  settled range and archives its final short chunk; a window whose end is
  not in the past still defers its tail to the live loop.

Re-running a finished window is cheap by construction: every clip is a key
the uploader skips when present, so an interrupted backfill resumes by
re-issuing the same window.
"""
import logging
import time

import frigate_s3_archiver as archiver
import logging_setup
import metrics

log = logging.getLogger("backfill")

# A window ending closer to now than this touches the live edge, and its tail
# is deferred exactly as the live loop defers its own; an older window is
# settled and archives its tail outright.
LIVE_EDGE_MARGIN_SECONDS = archiver.DEFAULT_MAX_GAP_SECONDS


def parse_bound(value, name):
    """An operator-supplied timestamp: epoch seconds or ISO 8601 date-time.

    Epoch seconds because every other time in this project is epoch seconds;
    ISO because nobody reads a wall clock in epoch. A trailing 'Z' means UTC,
    a zone offset is honored, a naive value is read as UTC - and the runner
    logs every bound back through human_ts in the key timezone, so the
    operator sees the instant that was actually meant.
    """
    raw = str(value).strip()
    if not raw:
        raise ValueError(f"{name}: is empty")
    try:
        return float(raw)
    except ValueError:
        pass
    from datetime import datetime, timezone

    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise ValueError(
            f"{name}: {value!r} is not a timestamp: use epoch seconds or an"
            " ISO 8601 date-time, e.g. 2026-10-01T00:00:00+03:00"
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


async def backfill_camera(
    camera,
    window_start_ts,
    window_end_ts,
    frigate_client,
    s3_bucket,
    s3_client=None,
    key_timezone=archiver.DEFAULT_S3_KEY_TIMEZONE,
    part_size=archiver.DEFAULT_PART_SIZE,
    http_chunk_size=archiver.DEFAULT_HTTP_CHUNK_SIZE,
    http_timeout=archiver.DEFAULT_HTTP_TIMEOUT,
    clip_retries=archiver.DEFAULT_CLIP_RETRIES,
    clip_retry_max_delay=archiver.DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS,
    max_gap_seconds=archiver.DEFAULT_MAX_GAP_SECONDS,
    interval_chunk_seconds=archiver.DEFAULT_INTERVAL_CHUNK_SECONDS,
    chunk_seconds=archiver.DEFAULT_RECORDINGS_CHUNK_SECONDS,
    overlap_seconds=archiver.DEFAULT_RECORDINGS_OVERLAP_SECONDS,
    motion_threshold=archiver.DEFAULT_MOTION_THRESHOLD,
    recordings_timeout=archiver.DEFAULT_RECORDINGS_TIMEOUT,
    key_prefix="",
    now_fn=time.time,
):
    """Archive one camera's motion intervals inside [start, end).

    Returns the ``upload_intervals`` result; raises on the first failed clip
    or Frigate error, like the live pass - a half-archived window must not
    report success.
    """
    settled_end = window_end_ts <= now_fn() - LIVE_EDGE_MARGIN_SECONDS
    intervals = await archiver.find_motion_intervals(
        camera=camera,
        after_ts=window_start_ts,
        end_ts=window_end_ts,
        frigate_client=frigate_client,
        chunk_seconds=chunk_seconds,
        overlap_seconds=overlap_seconds,
        motion_threshold=motion_threshold,
        max_gap_seconds=max_gap_seconds,
        interval_chunk_seconds=interval_chunk_seconds,
        recordings_timeout=recordings_timeout,
        min_start_ts=window_start_ts,
        settled_end=settled_end,
    )
    log.info(
        "%s: %d motion interval(s) in %s..%s (%s)",
        camera,
        len(intervals),
        archiver.human_ts(window_start_ts, key_timezone),
        archiver.human_ts(window_end_ts, key_timezone),
        "settled window" if settled_end else "window touches now",
    )
    return await archiver.upload_intervals(
        intervals,
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


async def run_backfill(
    config,
    cameras,
    window_start_ts,
    window_end_ts,
    s3_client=None,
    frigate_client_factory=None,
    now_fn=time.time,
):
    """One sequential pass over ``cameras``; returns a per-camera report.

    Sequential on purpose: the upload path's memory model sizes process peak
    by concurrent uploaders, and a manual catch-up has no latency target, so
    the schedule that cannot surprise a sidecar's memory limit wins. A camera
    that fails does not stop the others - re-running the same window re-does
    only what is missing, because present clips skip.
    """
    factory = frigate_client_factory or (
        lambda: archiver.make_frigate_client(
            config.frigate_url,
            username=config.frigate_user,
            password=config.frigate_password,
        )
    )
    if s3_client is None:
        s3_client = archiver.make_s3_client()

    report = {}
    for camera in cameras:
        logging_setup.bind_camera(camera)
        try:
            uploaded, skipped, last_end, truncated = await backfill_camera(
                camera=camera,
                window_start_ts=window_start_ts,
                window_end_ts=window_end_ts,
                frigate_client=factory(),
                s3_bucket=config.s3_bucket,
                s3_client=s3_client,
                key_timezone=config.s3_key_timezone,
                part_size=config.s3_part_size,
                http_timeout=config.http_timeout,
                clip_retries=config.clip_retries,
                clip_retry_max_delay=config.clip_retry_max_delay_seconds,
                max_gap_seconds=config.max_gap_seconds,
                key_prefix=config.s3_prefix,
                now_fn=now_fn,
            )
            report[camera] = {
                "uploaded": uploaded,
                "skipped": skipped,
                "truncated": truncated,
                "last_end": last_end,
            }
            log.info(
                "%s: backfill finished - %d uploaded, %d already present,"
                " %d archived truncated",
                camera, uploaded, skipped, truncated,
            )
        except Exception as exc:
            kind = metrics.record_error(camera, exc)
            report[camera] = {"error": kind, "detail": str(exc)}
            log.error("%s: backfill failed (%s): %s", camera, kind, exc)
    logging_setup.bind_camera("")
    return report
