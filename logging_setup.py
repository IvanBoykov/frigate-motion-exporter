"""Logging setup: plain text today, JSON tomorrow, same call sites.

Everything logs through the standard :mod:`logging`. The one thing this
module adds is a ``camera`` field: per-camera loggers are created with
``camera_logger(name)``, and the format string renders the field itself, so
message texts stay free of the ``[camera]`` boilerplate and a later JSON
formatter gets the camera as a real field instead of having to parse it out.

Output goes to stderr, the conventional place for program logs; stdout stays
empty for whatever a future caller might want to pipe.

A formatter owns its whole line, so the JSON formatter is a switch here and
nothing at the call sites changes.
"""
import contextvars
import json
import logging
import sys

DEFAULT_LOG_LEVEL = "INFO"

# The camera the running context belongs to. Each camera task gets a copy of
# this context - asyncio copies contexts into Tasks and into to_thread
# workers - so binding once at supervisor start tags every line that camera
# produces, including lines logged inside S3/Frigate worker threads, without
# threading a logger through five layers of function signatures.
_camera_var = contextvars.ContextVar("frigate_camera", default="")

# Valid values of LOG_LEVEL. "warn" is accepted as a spelling of WARNING
# because that is what it is called in every other log level list on earth.
LOG_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


class CameraFormatter(logging.Formatter):
    """``time level camera message``, the shape every log line has today.

    ``camera`` is empty on a message logged outside a camera, which renders
    as two spaces where the field would sit; the field is there so the JSON
    formatter can emit it without reparsing the text.
    """

    def format(self, record):
        record.camera = getattr(record, "camera", "") or _camera_var.get()
        return super().format(record)


class JsonFormatter(logging.Formatter):
    """One JSON object per line: ts, level, logger, camera, message, fields.

    A structured record in ``extra={"structured": {...}}`` is merged into the
    object, so a future machine-readable log carries its data as real fields
    without anybody parsing the message text.
    """

    def format(self, record):
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        camera = getattr(record, "camera", None) or _camera_var.get()
        if camera:
            payload["camera"] = camera
        structured = getattr(record, "structured", None)
        if structured:
            payload.update(structured)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def parse_log_level(value, default=DEFAULT_LOG_LEVEL):
    """Turn a LOG_LEVEL value into a logging level.

    Raises ValueError naming the allowed set: an unreadable level name should
    stop the process at startup, not silently log at a different verbosity
    than its operator asked for.
    """
    if value is None or not str(value).strip():
        return LOG_LEVELS[default]
    try:
        return LOG_LEVELS[str(value).strip().upper()]
    except KeyError:
        raise ValueError(
            f"unknown log level {value!r} "
            f"(expected one of {', '.join(sorted(set(LOG_LEVELS)))})"
        )


def configure_logging(level=logging.INFO, json_format=False, stream=None):
    """Point the root logger at stderr at ``level``; return the handler.

    Replaces the handlers logging would otherwise leave in place, so calling
    this twice (tests, embedding) does not double every line.
    """
    formatter = (
        JsonFormatter()
        if json_format
        else CameraFormatter("%(asctime)s %(levelname)-7s %(camera)-14s %(message)s")
    )
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
        existing.close()
    root.addHandler(handler)
    root.setLevel(level)
    return handler


def bind_camera(camera):
    """Attribute every log line of the running context to ``camera``.

    Called once per camera supervisor, before its camera task is created:
    the task and every thread asyncio later spawns from it inherit the
    binding, so no call site passes the camera around and no message text
    carries a ``[camera]`` prefix. Structured output gets the camera as a
    real field, not something to parse back out of the message.
    """
    _camera_var.set(camera)
