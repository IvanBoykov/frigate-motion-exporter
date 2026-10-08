"""Tests for the logging layer: levels, formats, and camera attribution."""
import asyncio
import io
import json
import logging

import pytest

import logging_setup


@pytest.fixture
def log_stream():
    """Capture log lines during the test, then restore the ambient state.

    configure_logging replaces the root handlers, so without a restore every
    later test would start logging to a leftover handler.
    """
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    stream = io.StringIO()
    logging_setup.configure_logging(level=logging.DEBUG, stream=stream)
    try:
        yield stream
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        logging_setup._camera_var.set("")


def json_lines(stream):
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_parse_log_level_accepts_the_documented_spellings():
    assert logging_setup.parse_log_level("debug") == logging.DEBUG
    assert logging_setup.parse_log_level(" WARNING ") == logging.WARNING
    assert logging_setup.parse_log_level("warn") == logging.WARNING
    assert logging_setup.parse_log_level("") == logging.INFO
    assert logging_setup.parse_log_level(None) == logging.INFO
    assert logging_setup.parse_log_level(None, "DEBUG") == logging.DEBUG


def test_parse_log_level_rejects_an_invented_level():
    with pytest.raises(ValueError) as excinfo:
        logging_setup.parse_log_level("trace")
    assert "trace" in str(excinfo.value)
    assert "DEBUG" in str(excinfo.value)


def test_level_filters_what_reaches_the_stream(log_stream):
    log = logging.getLogger("frigate_s3_archiver")
    logging_setup.configure_logging(level=logging.WARNING, stream=log_stream)
    log.info("idle loop sleeping")
    log.warning("clip cut off")

    out = log_stream.getvalue()
    assert "idle loop sleeping" not in out
    assert "clip cut off" in out


def test_configure_logging_replaces_its_previous_handler(log_stream):
    """Configuring twice must not double every line: the old handler goes."""
    later = io.StringIO()
    logging_setup.configure_logging(stream=later)
    logging.warning("logged once")

    assert "logged once" in later.getvalue()
    assert "logged once" not in log_stream.getvalue()


def test_text_line_carries_level_and_camera_as_a_field(log_stream):
    logging_setup.bind_camera("front-door")
    logging.getLogger("frigate_s3_archiver").warning("clip cut off")

    (line,) = log_stream.getvalue().splitlines()
    parts = line.split()
    # date, time, level, camera, then the message.
    assert parts[2] == "WARNING"
    assert parts[3] == "front-door"
    assert line.endswith("clip cut off")
    # The camera is a field, not a prefix baked into the message text.
    assert "[front-door]" not in line


def test_a_line_outside_a_camera_has_no_camera_field(log_stream):
    logging.getLogger("frigate_s3_archiver").info("Cameras: front, back")

    (line,) = log_stream.getvalue().splitlines()
    assert line.split()[2] == "INFO"
    assert line.endswith("Cameras: front, back")


def test_camera_binding_follows_its_own_task_into_worker_threads(log_stream):
    """Each camera's lines carry its own tag, including lines logged in a
    to_thread worker, where clip uploads are actually reported.

    The binding is a ContextVar: asyncio copies the context into every Task
    and into every to_thread worker, which is what keeps two concurrently
    archiving cameras from borrowing each other's tag.
    """
    logging_setup.configure_logging(
        level=logging.DEBUG, json_format=True, stream=log_stream
    )

    async def work(name):
        logging_setup.bind_camera(name)
        log = logging.getLogger("frigate_s3_archiver")
        await asyncio.sleep(0.01)
        log.info("pass finished by %s", name)
        await asyncio.to_thread(log.info, "upload finished by %s", name)

    async def scenario():
        await asyncio.gather(work("cam_a"), work("cam_b"))

    asyncio.run(scenario())

    lines = [
        payload
        for payload in json_lines(log_stream)
        if payload["logger"] == "frigate_s3_archiver"
    ]
    assert len(lines) == 4
    # Every line is tagged with the camera named in its own message.
    assert all(
        payload["camera"] == payload["message"].rsplit(" ", 1)[1]
        for payload in lines
    )
    assert [p["camera"] for p in lines].count("cam_a") == 2
    assert [p["camera"] for p in lines].count("cam_b") == 2


def test_json_formatter_emits_a_structured_record(log_stream):
    logging_setup.configure_logging(
        level=logging.DEBUG, json_format=True, stream=log_stream
    )
    logging_setup.bind_camera("back-yard")
    logging.getLogger("frigate_s3_archiver").info(
        "uploaded clip",
        extra={"structured": {"key": "back-yard/2026/01/01/00/00-00.mp4", "bytes": 12}},
    )

    (payload,) = json_lines(log_stream)
    assert payload["level"] == "INFO"
    assert payload["camera"] == "back-yard"
    assert payload["message"] == "uploaded clip"
    assert payload["key"].endswith("00-00.mp4")
    assert payload["bytes"] == 12
    assert payload["ts"].startswith("20")


def test_json_formatter_keeps_a_traceback_in_one_field(log_stream):
    logging_setup.configure_logging(
        level=logging.DEBUG, json_format=True, stream=log_stream
    )
    try:
        raise ValueError("bad segment")
    except ValueError:
        logging.getLogger("frigate_s3_archiver").error(
            "fatal path", exc_info=True
        )

    (payload,) = json_lines(log_stream)
    assert payload["level"] == "ERROR"
    assert "ValueError: bad segment" in payload["exc"]
    assert payload["exc"].count("Traceback") == 1
