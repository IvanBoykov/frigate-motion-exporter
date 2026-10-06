"""Prometheus metrics for the archiver: what was archived, how fast Frigate
answered, and - the part worth alerting on - how many of which errors.

Prometheus variable names use their domain meaning; the `*_total` counters are
cumulative by convention.
"""
import asyncio
import os

import requests
from botocore.exceptions import ClientError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

DEFAULT_METRICS_PORT = 9108
DEFAULT_METRICS_BIND = "0.0.0.0"

# Response times of the two Frigate call shapes: small JSON answers, and clip
# downloads whose duration is transfer-bound. One histogram, labeled, so a slow
# clip endpoint cannot hide behind fast config calls.
FRIGATE_RESPONSE_SECONDS = Histogram(
    "frigate_response_seconds",
    "Time spent waiting for a Frigate response, by request kind",
    ["kind"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)

CLIPS_UPLOADED_TOTAL = Counter(
    "clips_uploaded_total",
    "Clips written to S3",
    ["camera"],
)
CLIP_BYTES_UPLOADED_TOTAL = Counter(
    "clip_bytes_uploaded_total",
    "Bytes of clip data written to S3",
    ["camera"],
)
CLIPS_SKIPPED_TOTAL = Counter(
    "clips_skipped_total",
    "Clips already present in S3 and not re-uploaded",
    ["camera"],
)
CLIPS_TRUNCATED_UPLOADED_TOTAL = Counter(
    "clips_truncated_uploaded_total",
    "Clips whose stream was truncated on every download attempt and which"
    " were archived under a -truncated key instead of being lost",
    ["camera"],
)
CLIPS_UNAVAILABLE_UPLOADED_TOTAL = Counter(
    "clips_unavailable_uploaded_total",
    "Windows for which Frigate reported no recordings on every attempt and"
    " which were closed by a -no-recordings marker instead of pinning the"
    " watermark forever",
    ["camera"],
)

# Every error that ends a camera pass or a camera task, by camera and by error
# kind. Kinds come from a fixed vocabulary in error_kind(), except S3 API
# errors, which carry the S3 error code as `s3_<code>` - an alert on
# s3_accessdenied is worth having and cannot be enumerated in advance. Alert on
# a rise in errors_total by kind; a new kind appearing is itself a signal.
ERRORS_TOTAL = Counter(
    "errors_total",
    "Failures observed, by camera and error kind",
    ["camera", "kind"],
)
CAMERA_RESTARTS_TOTAL = Counter(
    "camera_restarts_total",
    "Times a camera task died and was restarted by the supervisor",
    ["camera"],
)
CAMERA_TASK_RUNNING = Gauge(
    "camera_task_running",
    "1 while a camera task has resolved its watermark and is working; 0 while"
    " it starts up, waits on the watermark read, or its supervisor backs off",
    ["camera"],
)
CAMERA_CONSECUTIVE_FAILURES = Gauge(
    "camera_consecutive_failures",
    "Failures of a camera task since its last healthy pass",
    ["camera"],
)

_metrics_httpd = None

_S3_NETWORK_KINDS = {
    "ReadTimeoutError": "s3_timeout",
    "ConnectTimeoutError": "s3_timeout",
    "EndpointConnectionError": "s3_unreachable",
    "ConnectionClosedError": "s3_connection_reset",
    "ConnectionError": "s3_connection_reset",
    "HTTPClientError": "s3_connection_reset",
    "IncompleteReadError": "s3_short_response",
}

# Fixed vocabulary: an alert rule can enumerate these. Anything unrecognized
# becomes "unknown", which is itself a signal that this list needs updating.
_ERROR_KINDS = frozenset(
    {
        "frigate_timeout",
        "frigate_unreachable",
        "frigate_http_4xx",
        "frigate_http_5xx",
        "frigate_short_response",
        "frigate_bad_payload",
        "frigate_empty_clip",
        "frigate_clip_truncated",
        "frigate_no_recordings",
        "task_returned",
        "cancelled",
        "timeout",
        "unknown",
    }
) | set(_S3_NETWORK_KINDS.values())


def frigate_error_kind(exc):
    try:
        status = exc.response.status_code
    except AttributeError:
        return None
    if 400 <= status < 500:
        return "frigate_http_4xx"
    if 500 <= status:
        return "frigate_http_5xx"
    return "frigate_bad_payload"


def s3_error_kind(exc):
    try:
        code = str(exc.response["Error"]["Code"])
    except (AttributeError, KeyError, TypeError):
        return None
    normalized = code.replace("-", "_").lower()
    return f"s3_{normalized}"


def _kind_of_one(exc):
    if isinstance(exc, asyncio.CancelledError):
        # not an Exception subclass: it must be checked before the rest, or a
        # cancelled upload would be reported as "unknown".
        return "cancelled"
    if isinstance(exc, EmptyClipError):
        return "frigate_empty_clip"
    if isinstance(exc, ClipTruncatedError):
        # the mfra trailer is missing: the export died mid-stream
        return "frigate_clip_truncated"
    if isinstance(exc, NoRecordingsError):
        return "frigate_no_recordings"
    if isinstance(exc, requests.exceptions.Timeout):
        return "frigate_timeout"
    if isinstance(exc, requests.exceptions.ChunkedEncodingError):
        # the body ended early: Frigate answered, then stopped mid-clip
        return "frigate_short_response"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "frigate_unreachable"
    if isinstance(exc, requests.exceptions.HTTPError):
        return frigate_error_kind(exc)
    if isinstance(exc, ClientError):
        return s3_error_kind(exc)
    if type(exc).__name__ in _S3_NETWORK_KINDS:
        return _S3_NETWORK_KINDS[type(exc).__name__]
    if isinstance(exc, asyncio.TimeoutError):
        return "timeout"
    if isinstance(exc, ValueError):
        return "frigate_bad_payload"
    return None


def error_kind(exc):
    """Classify ``exc`` into the fixed vocabulary.

    Our own failures wrap the transport error that caused them, so the chain is
    walked and the outermost classifiable error wins: the ``RuntimeError`` we
    raise around a failed scan is not a failure kind of its own, so a scan that
    failed because Frigate answered 500 is counted as ``frigate_http_5xx``, not
    as an internal error.
    """
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        kind = _kind_of_one(current)
        if kind is not None:
            return kind
        current = current.__cause__ or current.__context__
    return "unknown"


def record_error(camera, exc):
    kind = error_kind(exc)
    ERRORS_TOTAL.labels(camera=camera, kind=kind).inc()
    return kind


def record_failure(camera, kind):
    """Count a failure that is not an exception (a task that just returned)."""
    if kind not in _ERROR_KINDS:
        raise ValueError(f"unknown error kind: {kind}")
    ERRORS_TOTAL.labels(camera=camera, kind=kind).inc()
    return kind


class EmptyClipError(RuntimeError):
    """Frigate answered 200 with no clip data.

    The 200 is committed before ffmpeg runs, so an export that dies before
    its first packet - usually because the files the database lists cannot be
    opened - arrives as a successful empty response. Same unavailable-window
    fault as NoRecordingsError, one layer below it.
    """


class ClipTruncatedError(RuntimeError):
    """The clip stream ended before FFmpeg wrote its mfra trailer.

    The body is cut somewhere inside a fragment: whatever arrived is
    playable but shorter than the requested interval. Retrying is the only
    remedy; re-requesting the same window from Frigate usually succeeds once
    the export fault passes.
    """


class NoRecordingsError(RuntimeError):
    """Frigate reports that no recordings exist for the requested window.

    The scan had just listed segments for this window, so this is a
    database-versus-disk disagreement inside Frigate: the files are gone and
    no retry will bring them back. The window is closed by a marker object
    instead of pinning the watermark forever.
    """


def start_metrics_server(port=DEFAULT_METRICS_PORT, bind=DEFAULT_METRICS_BIND):
    """Serve /metrics on its own port; returns the bound port.

    ``port=0`` asks the OS for a free port, which is what tests use.
    """
    if port < 0:
        raise ValueError("port must be >= 0")

    global _metrics_httpd
    httpd, _thread = start_http_server(int(port), addr=bind)
    _metrics_httpd = httpd
    return httpd.server_address[1]


def shutdown_metrics_server():
    """Stop the /metrics listener; used by tests and shutdown paths."""
    global _metrics_httpd
    if _metrics_httpd is None:
        return
    _metrics_httpd.shutdown()
    _metrics_httpd.server_close()
    _metrics_httpd = None


def metrics_port_from_env(default=DEFAULT_METRICS_PORT):
    raw = os.environ.get("METRICS_PORT", "")
    return int(raw) if raw.strip() else int(default)


def metrics_bind_from_env(default=DEFAULT_METRICS_BIND):
    return os.environ.get("METRICS_BIND", "").strip() or default
