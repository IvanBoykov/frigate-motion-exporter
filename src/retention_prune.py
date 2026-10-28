"""The prune pass: census, plan, delete, repeat on an interval.

This is the only place the retention modules meet, and the only place retention
touches metrics. It runs as its own supervised task rather than inside a camera
pass for three reasons:

- a size target is a property of the whole archive, so no one camera can decide
  it;
- a camera task that raises is restarted, and that restart means "archiving is
  broken". A pruner that cannot list its bucket is not that, so it reports on
  the ``prune_*`` series and leaves ``camera_restarts_total`` alone;
- Frigate being down must not stop deletion and S3 being down must not stop
  archiving. Separate failure domains, separate supervisors.

S3 calls go through ``asyncio.to_thread`` like the upload path, and the delete
worker gets a cancellation event the same way the uploader does: ``to_thread``
cannot interrupt a thread, so a cancelled pass would otherwise keep deleting for
minutes after its own task was detached.
"""
import asyncio
import logging
import math
import threading
import time
from datetime import datetime

import archive_census
import metrics
import retention
import retention_delete

log = logging.getLogger("retention_prune")


def restart_delay(failures, base, maximum):
    """Seconds to wait after ``failures`` consecutive failed passes.

    Same doubling-and-saturate shape the camera supervisor uses, reimplemented
    here rather than imported: the archiver imports this module to start the
    prune task, so reaching back into it would be a cycle.
    """
    if failures <= 0:
        raise ValueError("failures must be > 0")
    exponent = min(failures - 1, 1023)
    return min(float(base) * (2**exponent), float(maximum))


def growth_hours(prune_interval_seconds):
    """How many hour folders of growth the target must leave room for.

    ``+ 1`` because the newest hour in the bucket is the one still being
    written: a pass that runs ten minutes into an hour measures a tenth of it,
    and sizing the estimate on that alone would predict the interval's writes at
    a fraction of their real size.
    """
    if prune_interval_seconds <= 0:
        raise ValueError("prune_interval_seconds must be > 0")
    return int(math.ceil(prune_interval_seconds / 3600.0)) + 1


def _gib(value):
    return value / 2**30


def log_plan(plan, dry_run, key_timezone):
    """One summary line for the pass and one per camera naming what survives.

    The per-camera line is the only place the log says what the archive will
    look like: the freed total says what left, the horizon says what stayed.
    The same line is written in a dry run with "would be", which is the whole
    point of running one before enabling deletion.
    """
    log.info(
        "Retention pass (%s): %d folders, %.2f GiB%s; archive %.2f GiB,"
        " target %s",
        plan.reason,
        len(plan.folders),
        _gib(plan.freed_bytes),
        " planned, not deleted" if dry_run else " freed",
        _gib(plan.current_bytes),
        "not set" if plan.target_bytes is None else f"{_gib(plan.target_bytes):.2f} GiB",
    )
    if plan.over_budget_bytes:
        log.warning(
            "Archive stays %.2f GiB over its target (%s). This does not"
            " resolve by itself: the configuration and the bucket disagree.",
            _gib(plan.over_budget_bytes),
            plan.reason,
        )

    verb = "would keep its oldest clip at" if dry_run else "oldest clip now at"
    for camera, report in sorted(plan.cameras.items()):
        if report.horizon_epoch is None:
            log.info("%s: no clips in the archive", camera)
            continue
        oldest = datetime.fromtimestamp(
            report.horizon_epoch, tz=key_timezone
        ).strftime("%Y/%m/%d/%H")
        log.info(
            "%s: %s %s (%d protected hours)",
            camera,
            verb,
            oldest,
            report.protected_hours,
        )


def set_census_metrics(census, plan, dry_run):
    """Publish the census and the plan.

    The horizon gauge describes the plan, not the bucket, and it is set for a
    dry run too: a dry run exists to be reviewed before deletion is enabled, and
    a gauge still reporting the footage the plan intends to remove cannot be
    reviewed against anything.
    """
    metrics.RETENTION_DRY_RUN.set(1 if dry_run else 0)
    metrics.RETENTION_ARCHIVE_BYTES.set(census.total_bytes)
    metrics.RETENTION_TARGET_BYTES.set(
        0 if plan.target_bytes is None else plan.target_bytes
    )
    metrics.RETENTION_HEADROOM_BYTES.set(plan.headroom_bytes)

    for camera, stat in census.cameras.items():
        metrics.ARCHIVE_BYTES.labels(camera=camera).set(stat.bytes)
    for camera, report in plan.cameras.items():
        if report.horizon_epoch is not None:
            metrics.ARCHIVE_HORIZON_TIMESTAMP_SECONDS.labels(camera=camera).set(
                report.horizon_epoch
            )

    for reason in (
        retention.REASON_TARGET_MET,
        retention.REASON_FLOOR,
        retention.REASON_PERCENT_CAP,
        retention.REASON_FOREIGN_KEYS,
        retention.REASON_HEADROOM,
    ):
        metrics.RETENTION_OVER_BUDGET_BYTES.labels(reason=reason).set(
            plan.over_budget_bytes if plan.reason == reason else 0
        )


def count_deletes(per_camera):
    for camera, (keys, freed) in per_camera.items():
        if keys:
            metrics.CLIPS_DELETED_TOTAL.labels(camera=camera).inc(keys)
        if freed:
            metrics.DELETED_BYTES_TOTAL.labels(camera=camera).inc(freed)


async def run_prune_pass(
    s3_client,
    bucket,
    prefix="",
    key_timezone=None,
    keep_days=None,
    keep_bytes=None,
    prune_interval_seconds=retention.DEFAULT_RETENTION_PRUNE_INTERVAL_SECONDS,
    max_delete_percent=retention.DEFAULT_RETENTION_MAX_DELETE_PERCENT,
    max_unknown_fraction=retention.DEFAULT_RETENTION_MAX_UNKNOWN_FRACTION,
    growth_headroom_bytes=None,
    dry_run=True,
    now_fn=time.time,
):
    """One census -> plan -> delete cycle; returns the plan.

    A census failure propagates rather than being skipped: an unknown archive
    size is not a licence to delete anything, and the supervisor's backoff is
    the right response to an S3 that will not answer a listing.
    """
    floor_hours = retention.HOURS_PER_DAY * int(keep_days or 0)
    hours = growth_hours(prune_interval_seconds)

    metrics.PRUNE_IN_PROGRESS.set(1)
    try:
        census = await asyncio.to_thread(
            archive_census.take_census,
            s3_client,
            bucket,
            prefix,
            key_timezone,
            floor_hours,
            hours,
        )
        headroom = (
            int(growth_headroom_bytes)
            if growth_headroom_bytes is not None
            else archive_census.growth_headroom(census, hours)
        )
        plan = retention.build_plan(
            census,
            keep_bytes=keep_bytes,
            headroom_bytes=headroom,
            max_delete_percent=max_delete_percent,
            max_unknown_fraction=max_unknown_fraction,
        )
        set_census_metrics(census, plan, dry_run)
        log_plan(plan, dry_run, key_timezone)

        if plan.folders and dry_run:
            log.info(
                "Dry run: %.2f GiB was NOT deleted. Set"
                " RETENTION_DRY_RUN=false to allow deletion.",
                _gib(plan.freed_bytes),
            )
        elif plan.folders:
            cancel_event = threading.Event()
            try:
                per_camera, freed, failed = await asyncio.to_thread(
                    retention_delete.execute_plan,
                    s3_client,
                    bucket,
                    plan,
                    prefix,
                    key_timezone,
                    cancel_event,
                )
            except asyncio.CancelledError:
                cancel_event.set()
                raise
            count_deletes(per_camera)
            log.info(
                "Retention deleted %d keys / %.2f GiB",
                sum(keys for keys, _ in per_camera.values()),
                _gib(freed),
            )
            if failed:
                log.error(
                    "%d keys were reported as not deleted; they stay for the"
                    " next pass",
                    len(failed),
                )

        metrics.PRUNE_LAST_SUCCESS_TIMESTAMP_SECONDS.set(now_fn())
        return plan
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.error(
            "Retention pass failed (%s)",
            metrics.record_prune_failure(exc),
            exc_info=True,
        )
        raise
    finally:
        metrics.PRUNE_IN_PROGRESS.set(0)


async def supervise_prune(
    pass_fn,
    interval_seconds,
    backoff_seconds=retention.DEFAULT_PRUNE_BACKOFF_SECONDS,
    backoff_max_seconds=retention.DEFAULT_PRUNE_BACKOFF_MAX_SECONDS,
    sleep=asyncio.sleep,
):
    """Run a prune pass forever: the interval after success, backoff after error.

    The success clock is the interval, so a healthy pruner lists the bucket once
    per interval and not once per loop; the failure clock is the same doubling
    backoff the camera supervisor uses, so a pruner facing an unreachable S3
    stops hammering it without the process giving up on retention.
    """
    failures = 0
    while True:
        try:
            await pass_fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            failures += 1
            delay = restart_delay(failures, backoff_seconds, backoff_max_seconds)
            log.warning(
                "Retention pass failed %d time(s) in a row; next pass in %.0f s",
                failures,
                delay,
            )
            await sleep(delay)
        else:
            failures = 0
            await sleep(interval_seconds)


async def run_pruner(s3_client, config, key_timezone, now_fn=time.time):
    """The prune pass as ``supervise_prune`` wants it, wired from the config."""
    return await run_prune_pass(
        s3_client,
        config.s3_bucket,
        prefix=config.s3_prefix,
        key_timezone=key_timezone,
        keep_days=config.retention_keep_days,
        keep_bytes=config.retention_keep_bytes,
        prune_interval_seconds=config.retention_prune_interval_seconds,
        max_delete_percent=config.retention_max_delete_percent,
        max_unknown_fraction=config.retention_max_unknown_fraction,
        growth_headroom_bytes=config.retention_growth_headroom_bytes,
        dry_run=config.retention_dry_run,
        now_fn=now_fn,
    )
