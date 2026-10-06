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
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    )
