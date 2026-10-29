"""Environment configuration, read once at startup.

Business functions keep taking plain typed arguments; this module is the only
place that reads tuning knobs from the environment. Every value is parsed and
validated here so a typo fails the process at startup with the variable name in
the message, rather than surfacing as a truncated download or a bad S3 key
after the first clip was already streamed.

Defaults are not duplicated here: they come from the ``DEFAULT_*`` constants in
:mod:`frigate_s3_archiver`, so there is one source of truth for what happens
when a variable is unset. The import is one-way - the archiver reaches this
module from ``main()`` only, otherwise the two would form a cycle.
"""
import os
import re
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import logging_setup
import retention

import frigate_s3_archiver as defaults


class ConfigError(SystemExit):
    """A configured value is unusable; the message names the variable."""

    def __init__(self, variable, detail):
        super().__init__(f"{variable}: {detail}")
        self.variable = variable
        self.detail = detail


def _present(environ, name):
    """The stripped value of ``name``, or None when unset or blank."""
    raw = environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _number(environ, name, default, kind=float, minimum=None, maximum=None):
    raw = _present(environ, name)
    if raw is None:
        return default
    try:
        value = kind(raw)
    except ValueError:
        raise ConfigError(name, f"expected {kind.__name__}, got {raw!r}")
    if minimum is not None and value < minimum:
        raise ConfigError(name, f"expected >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ConfigError(name, f"expected <= {maximum}, got {value}")
    return value


def _seconds(environ, name, default, minimum=0.0):
    return _number(environ, name, default, float, minimum)


def _flag(environ, name, default):
    """A boolean knob: unset means ``default``; a set value must be an
    accepted spelling of true or false rather than a guess."""
    raw = _present(environ, name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ConfigError(
        name,
        f"expected a boolean (true/false), got {raw!r}",
    )


def _bytes(environ, name, minimum=None):
    """An optional byte count: unset/blank is None, a set value is an int >=
    ``minimum`` (a set 0 is a value - "reserve nothing" - not a missing knob).

    Both retention thresholds are optional in a way no other knob is - unset
    means "this threshold is not configured", not "use a default" - so they get
    their own reader instead of a sentinel default.
    """
    raw = _present(environ, name)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(name, f"expected an integer byte count, got {raw!r}")
    if minimum is not None and value < minimum:
        raise ConfigError(name, f"expected >= {minimum}, got {value}")
    return value


def _retention(environ):
    """Parse and cross-check the retention knobs.

    Retention is off when neither threshold is set, and off means the prune
    task is never started. ``RETENTION_KEEP_DAYS=0`` is a legal threshold that
    means "no day floor" - the operator allowed keeping less than a day - and
    the only thing left holding the archive back is the size target plus the
    newest hour folder of every camera, which the census never offers.

    Three combinations are refused here rather than discovered at runtime,
    because each describes a configuration whose effect can only be surprising:

    - deletion enabled (``RETENTION_DRY_RUN=false``) with no threshold at all:
      the operator asked for removals but never said what to keep.
    - ``RETENTION_KEEP_BYTES`` without ``RETENTION_KEEP_DAYS``: a size target
      alone has no age limit, and the day threshold is the one place the age
      bound is stated. ``0`` is the explicit way to say "no floor"; an unset
      variable is not.
    - a negative day count, which is not a threshold.
    """
    keep_days = _number(
        environ, "RETENTION_KEEP_DAYS", None, int, minimum=None
    )
    keep_bytes = _bytes(environ, "RETENTION_KEEP_BYTES", minimum=1)
    dry_run = _flag(environ, "RETENTION_DRY_RUN", True)

    if keep_days is not None and keep_days < 0:
        raise ConfigError(
            "RETENTION_KEEP_DAYS",
            f"must be >= 0; {keep_days} is not a depth. 0 means no day floor",
        )
    if keep_bytes is not None and keep_days is None:
        raise ConfigError(
            "RETENTION_KEEP_DAYS",
            "RETENTION_KEEP_DAYS is required when RETENTION_KEEP_BYTES is"
            " set. Set it to the depth you want protected, or to 0 for no day"
            " floor (size alone then decides, down to each camera's newest"
            " hour)",
        )
    if not dry_run and keep_days is None and keep_bytes is None:
        raise ConfigError(
            "RETENTION_DRY_RUN",
            "RETENTION_DRY_RUN=false enables deletion, but no threshold is"
            " configured - set RETENTION_KEEP_DAYS or RETENTION_KEEP_BYTES"
            " first. Leaving RETENTION_DRY_RUN at its default true keeps"
            " retention planning only",
        )

    percent = _number(
        environ,
        "RETENTION_MAX_DELETE_PERCENT",
        retention.DEFAULT_RETENTION_MAX_DELETE_PERCENT,
        float,
        minimum=0.1,
        maximum=100.0,
    )
    unknown_fraction = _number(
        environ,
        "RETENTION_MAX_UNKNOWN_FRACTION",
        retention.DEFAULT_RETENTION_MAX_UNKNOWN_FRACTION,
        float,
        minimum=0.0,
        maximum=1.0,
    )
    prune_interval = _seconds(
        environ,
        "RETENTION_PRUNE_INTERVAL_SECONDS",
        retention.DEFAULT_RETENTION_PRUNE_INTERVAL_SECONDS,
        minimum=retention.MIN_RETENTION_PRUNE_INTERVAL_SECONDS,
    )

    return {
        "retention_keep_days": keep_days,
        "retention_keep_bytes": keep_bytes,
        "retention_max_delete_percent": percent,
        "retention_max_unknown_fraction": unknown_fraction,
        "retention_prune_interval_seconds": prune_interval,
        "retention_growth_headroom_bytes": _bytes(
            environ, "RETENTION_GROWTH_HEADROOM_BYTES", minimum=0
        ),
        "retention_dry_run": _flag(environ, "RETENTION_DRY_RUN", True),
        "retention_limit_upload_depth": _flag(
            environ, "RETENTION_LIMIT_UPLOAD_DEPTH", True
        ),
    }


def _log_level(environ, name, default):
    raw = _present(environ, name)
    try:
        return logging_setup.parse_log_level(raw, default)
    except ValueError as exc:
        raise ConfigError(name, str(exc))


def _camera_filter(environ, name):
    raw = _present(environ, name)
    if raw is None:
        return None
    try:
        return re.compile(raw)
    except re.error as exc:
        raise ConfigError(name, f"invalid regular expression: {exc}")


def _key_timezone(environ, name, default):
    raw = _present(environ, name)
    if raw is None:
        return default
    try:
        return ZoneInfo(raw)
    except (ZoneInfoNotFoundError, ValueError):
        raise ConfigError(
            name,
            f"unknown IANA timezone: {raw!r} "
            "(expected a name like Europe/Moscow or UTC)",
        )


def timezone_collapses_hours(zone, year):
    """True if ``zone`` has a DST fold in ``year``.

    An hour-keyed layout cannot name both copies of a repeated local hour: on
    a fall-back, two real hours render to one key and the second upload
    overwrites the first. A spring-forward gap is harmless - it skips a name,
    it does not reuse one - so the tested condition is exactly "two
    consecutive hours carry the same hour-of-day", which is a fold and only
    a fold.
    """
    previous = None
    start = datetime(year, 1, 1, tzinfo=zone).timestamp()
    for hour in range(366 * 24):
        moment = datetime.fromtimestamp(start + hour * 3600, zone)
        if moment.year != year:
            break
        if previous is not None and moment.hour == previous:
            return True
        previous = moment.hour
    return False


def _s3_prefix(environ, name):
    """An S3 key prefix, normalized to empty or 'one/or/more/'.

    Leading slashes would create an empty-named folder, and a missing trailing
    slash would glue the prefix onto the camera name.
    """
    raw = _present(environ, name)
    if raw is None:
        return ""
    stripped = raw.strip().strip("/")
    if not stripped:
        return ""
    if "//" in stripped:
        raise ConfigError(name, "must not contain empty path segments '//'")
    if any(part in (".", "..") for part in stripped.split("/")):
        raise ConfigError(name, "must not contain '.' or '..'")
    return stripped + "/"


@dataclass(frozen=True)
class Config:
    frigate_url: str
    frigate_user: str
    frigate_password: str
    s3_bucket: str
    s3_prefix: str
    s3_part_size: int
    s3_key_timezone: object
    cameras_include: object
    cameras_exclude: object
    max_gap_seconds: float
    idle_sleep_seconds: float
    http_timeout: float
    watermark_lookback_seconds: float
    first_run_lookback_seconds: float
    clip_retries: int
    clip_retry_max_delay_seconds: float
    camera_restart_backoff_seconds: float
    camera_restart_backoff_max_seconds: float
    retention_keep_days: object
    retention_keep_bytes: object
    retention_max_delete_percent: float
    retention_max_unknown_fraction: float
    retention_prune_interval_seconds: float
    retention_growth_headroom_bytes: object
    retention_dry_run: bool
    retention_limit_upload_depth: bool
    backfill_after: object
    backfill_before: object
    log_level: int
    log_json: bool

    @property
    def backfill_enabled(self):
        """Whether this process is a one-shot backfill instead of the loop.

        Both bounds or neither: the cross-check in load_config refuses half a
        window, so one flag is enough here.
        """
        return self.backfill_after is not None

    @property
    def retention_enabled(self):
        """Whether a prune task should run at all.

        Unset thresholds mean unset, not "delete everything": with neither set
        the archiver behaves exactly as it did before retention existed.
        """
        return (
            self.retention_keep_days is not None
            or self.retention_keep_bytes is not None
        )


def _backfill_window(environ):
    """The explicit [after, before) of a one-shot backfill, or (None, None).

    A backfill is the operator's answer to "the outage outlived the
    FIRST_RUN_LOOKBACK_SECONDS window": one pass over exactly these bounds,
    then exit. The window is parsed here (epoch seconds or ISO, via
    backfill.parse_bound) so a typo names its variable at startup rather
    than after the first clip.

    Cross-checks, all refused at startup:
    - only one bound: half a window is not a bound - the point of the mode
      is that a human said how far back;
    - after >= before: an empty or inverted window archives nothing while
      reading like a successful run;
    - backfill + RETENTION_DRY_RUN=false: one process would delete archive
      on a threshold computed against the archive before the fill, then
      write the clips back - both orderings lose, so the modes are separate
      processes and the operator runs them in the order they meant.
    """
    raw_after = _present(environ, "BACKFILL_AFTER")
    raw_before = _present(environ, "BACKFILL_BEFORE")
    if raw_after is None and raw_before is None:
        return None, None
    if raw_after is None or raw_before is None:
        missing = "BACKFILL_AFTER" if raw_after is None else "BACKFILL_BEFORE"
        raise ConfigError(
            missing,
            "a backfill window has two bounds; set " + missing
            + " (epoch seconds or ISO 8601) or unset both to run the"
            " normal loop",
        )
    import backfill

    try:
        after = backfill.parse_bound(raw_after, "BACKFILL_AFTER")
        before = backfill.parse_bound(raw_before, "BACKFILL_BEFORE")
    except ValueError as exc:
        variable, _, detail = str(exc).partition(":")
        raise ConfigError(variable, detail.strip()) from None
    if after >= before:
        raise ConfigError(
            "BACKFILL_AFTER",
            f"must be before BACKFILL_BEFORE; got a window"
            f" [{after!r}, {before!r}) that archives nothing",
        )
    if _flag(environ, "RETENTION_DRY_RUN", True) is False:
        raise ConfigError(
            "RETENTION_DRY_RUN",
            "backfill and enabled retention deletion cannot share one"
            " process: the prune pass would judge the old archive before"
            " the fill existed. Run the backfill first (dry-run retention"
            " is fine), then the normal process",
        )
    return after, before


def load_config(environ=None):
    """Read every tuning knob from the environment.

    ``environ`` exists for tests; production calls take the real environment.
    """
    return _load(os.environ if environ is None else environ)


def _load(environ):
    bucket = _present(environ, "S3_BUCKET")
    if not bucket:
        raise SystemExit(
            "S3_BUCKET is not set: name the bucket that holds clips "
            "(remaining S3 settings come from the boto3 chain, see README.md)"
        )

    include = _camera_filter(environ, "CAMERAS_INCLUDE")
    exclude = _camera_filter(environ, "CAMERAS_EXCLUDE")
    if include is not None and exclude is not None:
        raise ConfigError(
            "CAMERAS_INCLUDE",
            "CAMERAS_INCLUDE and CAMERAS_EXCLUDE are both set - "
            "only one of them is needed",
        )

    part_size = _number(
        environ,
        "S3_PART_SIZE",
        defaults.DEFAULT_PART_SIZE,
        int,
        minimum=defaults.MIN_MULTIPART_PART_SIZE,
    )
    backoff = _seconds(
        environ,
        "CAMERA_RESTART_BACKOFF_SECONDS",
        defaults.DEFAULT_CAMERA_RESTART_BACKOFF_SECONDS,
        minimum=0.1,
    )
    backoff_max = _seconds(
        environ,
        "CAMERA_RESTART_BACKOFF_MAX_SECONDS",
        defaults.DEFAULT_CAMERA_RESTART_BACKOFF_MAX_SECONDS,
        minimum=0.1,
    )
    if backoff_max < backoff:
        raise ConfigError(
            "CAMERA_RESTART_BACKOFF_MAX_SECONDS",
            f"must be >= CAMERA_RESTART_BACKOFF_SECONDS ({backoff})",
        )

    backfill_after, backfill_before = _backfill_window(environ)

    return Config(
        frigate_url=_present(environ, "FRIGATE_URL")
        or defaults.DEFAULT_FRIGATE_URL,
        frigate_user=environ.get("FRIGATE_USER", ""),
        frigate_password=environ.get("FRIGATE_PASSWORD", ""),
        s3_bucket=bucket,
        s3_prefix=_s3_prefix(environ, "S3_PREFIX"),
        s3_part_size=part_size,
        s3_key_timezone=_key_timezone(
            environ, "S3_KEY_TIMEZONE", defaults.DEFAULT_S3_KEY_TIMEZONE
        ),
        cameras_include=include,
        cameras_exclude=exclude,
        max_gap_seconds=_seconds(
            environ, "MAX_GAP_SECONDS", defaults.DEFAULT_MAX_GAP_SECONDS
        ),
        idle_sleep_seconds=_seconds(
            environ,
            "IDLE_SLEEP_SECONDS",
            defaults.DEFAULT_IDLE_SLEEP_SECONDS,
            minimum=1.0,
        ),
        http_timeout=_seconds(
            environ,
            "HTTP_TIMEOUT",
            defaults.DEFAULT_HTTP_TIMEOUT,
            minimum=1.0,
        ),
        watermark_lookback_seconds=_seconds(
            environ,
            "WATERMARK_LOOKBACK_SECONDS",
            defaults.DEFAULT_WATERMARK_LOOKBACK_SECONDS,
        ),
        first_run_lookback_seconds=_seconds(
            environ,
            "FIRST_RUN_LOOKBACK_SECONDS",
            defaults.DEFAULT_FIRST_RUN_LOOKBACK_SECONDS,
        ),
        clip_retries=_number(
            environ,
            "CLIP_RETRIES",
            defaults.DEFAULT_CLIP_RETRIES,
            int,
            0,
        ),
        clip_retry_max_delay_seconds=_seconds(
            environ,
            "CLIP_RETRY_MAX_DELAY_SECONDS",
            defaults.DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS,
        ),
        camera_restart_backoff_seconds=backoff,
        camera_restart_backoff_max_seconds=backoff_max,
        backfill_after=backfill_after,
        backfill_before=backfill_before,
        **_retention(environ),
        log_level=_log_level(environ, "LOG_LEVEL", logging_setup.DEFAULT_LOG_LEVEL),
        log_json=_flag(environ, "LOG_JSON", False),
    )
