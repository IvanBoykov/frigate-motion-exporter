"""Error classification, metric recording, and per-camera supervision."""
import asyncio
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import pytest
import requests
from botocore.exceptions import ClientError
from prometheus_client import REGISTRY

import frigate_s3_archiver as m
import metrics
from test_frigate_s3_archiver import (
    BASE_TS,
    SEGMENT_SECONDS,
    FakeFrigateHandler,
    make_segments,
)


@pytest.fixture()
def frigate_server():
    FakeFrigateHandler.segments_by_camera = {"metric_cam": make_segments([1, 1, 1])}
    FakeFrigateHandler.clip_failures = set()
    FakeFrigateHandler.requested_clips = []
    FakeFrigateHandler.clip_attempts = []
    FakeFrigateHandler.truncate_clip_attempts = 0
    FakeFrigateHandler.recording_requests = []
    FakeFrigateHandler.clip_bytes = 1024
    FakeFrigateHandler.clip_stream_delay = 0.0

    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeFrigateHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class CountingS3:
    """S3 double that remembers how many bytes it was handed."""

    class _Meta:
        endpoint_url = "https://s3.fake.local"
        region_name = "fake-region-1"

    meta = _Meta()

    def __init__(self):
        self.stored_bytes = 0
        self.keys = set()

    def head_object(self, Bucket, Key):
        if Key in self.keys:
            return {"ContentLength": 1024}
        raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def put_object(self, Bucket, Key, Body, ContentType):
        self.keys.add(Key)
        self.stored_bytes += len(Body)


def sample(name, **labels):
    value = REGISTRY.get_sample_value(name, labels)
    assert value is not None, f"{name} {labels} was never exported"
    return value


def observed(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0


def run(coroutine):
    return asyncio.run(coroutine)


def http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "PutObject")


def upload(camera, client, s3):
    return run(
        m.upload_clip_hybrid(
            camera=camera,
            start_ts=float(BASE_TS),
            end_ts=float(BASE_TS + SEGMENT_SECONDS),
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )[0]


# Camera names below are unique per test: the registry is process-global, and
# unique labels keep counter assertions independent of execution order.
def test_error_kind_covers_the_vocabulary():
    cases = [
        (requests.Timeout("t"), "frigate_timeout"),
        (requests.ConnectionError("c"), "frigate_unreachable"),
        (http_error(404), "frigate_http_4xx"),
        (http_error(503), "frigate_http_5xx"),
        (requests.exceptions.ChunkedEncodingError("short"), "frigate_short_response"),
        (metrics.EmptyClipError("empty"), "frigate_empty_clip"),
        (ValueError("bad json"), "frigate_bad_payload"),
        (client_error("NoSuchBucket"), "s3_nosuchbucket"),
        (client_error("AccessDenied"), "s3_accessdenied"),
        (asyncio.TimeoutError(), "timeout"),
        (asyncio.CancelledError(), "cancelled"),
        (KeyboardInterrupt(), "unknown"),
    ]
    for exc, kind in cases:
        assert metrics.error_kind(exc) == kind
        # S3 API errors carry the code S3 returned and cannot be enumerated in
        # advance; every other kind belongs to the fixed vocabulary.
        assert kind in metrics._ERROR_KINDS or kind.startswith("s3_")


def test_error_kind_walks_the_cause_chain():
    outer = RuntimeError("Failed to fetch Frigate recordings")
    outer.__cause__ = http_error(503)

    assert metrics.error_kind(outer) == "frigate_http_5xx"


def test_error_kind_survives_a_cause_cycle():
    exc = RuntimeError("cycle")
    exc.__cause__ = exc

    assert metrics.error_kind(exc) == "unknown"


def test_record_failure_rejects_an_invented_kind():
    with pytest.raises(ValueError):
        metrics.record_failure("metric_cam", "everything_is_fine")


def test_upload_records_clips_bytes_and_skips(frigate_server):
    camera = "metric_cam_upload"
    client = m.make_frigate_client(frigate_server, username="", password="")
    s3 = CountingS3()

    assert upload(camera, client, s3) is True
    assert upload(camera, client, s3) is False

    assert sample("clips_uploaded_total", camera=camera) == 1
    assert sample("clip_bytes_uploaded_total", camera=camera) == s3.stored_bytes
    assert sample("clips_skipped_total", camera=camera) == 1


def test_frigate_json_latency_is_observed(frigate_server):
    client = m.make_frigate_client(frigate_server, username="", password="")
    before = observed("frigate_response_seconds_count", kind="json")

    run(m.fetch_cameras(client))

    assert sample("frigate_response_seconds_count", kind="json") == before + 1


def test_frigate_clip_latency_is_observed(frigate_server):
    client = m.make_frigate_client(frigate_server, username="", password="")
    before = observed("frigate_response_seconds_count", kind="clip_ttfb")

    upload("metric_cam_clip", client, CountingS3())

    assert sample("frigate_response_seconds_count", kind="clip_ttfb") == before + 1


def test_metrics_endpoint_serves_prometheus_text():
    port = metrics.start_metrics_server(port=0)
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/metrics", timeout=5
        ) as response:
            body = response.read().decode()
    finally:
        metrics.shutdown_metrics_server()

    for name in (
        "clips_uploaded_total",
        "frigate_response_seconds",
        "errors_total",
        "camera_restarts_total",
        "camera_task_running",
    ):
        assert f"# TYPE {name}" in body


def test_restart_delay_grows_and_saturates():
    assert m.camera_restart_delay(1, base=5, maximum=300) == 5
    assert m.camera_restart_delay(3, base=5, maximum=300) == 20
    assert m.camera_restart_delay(10, base=5, maximum=300) == 300


def test_restart_delay_rejects_a_bad_counter():
    with pytest.raises(ValueError):
        m.camera_restart_delay(0)



def test_clip_retry_delay_doubles_until_the_cap():
    assert m.clip_retry_delay(0) == 10.0
    assert m.clip_retry_delay(1) == 20.0
    assert m.clip_retry_delay(2) == 40.0
    assert m.clip_retry_delay(3) == 80.0
    assert m.clip_retry_delay(50) == 80.0
    # the default schedule fits the Frigate-restart budget
    default_span = sum(
        m.clip_retry_delay(attempt) for attempt in range(m.DEFAULT_CLIP_RETRIES)
    )
    assert 300 <= default_span <= 360



def test_clip_retry_delay_never_overflows():
    assert m.clip_retry_delay(10**6, base=5, max_delay=300) == 300



def test_clip_retry_delay_rejects_bad_arguments():
    with pytest.raises(ValueError):
        m.clip_retry_delay(-1)
    with pytest.raises(ValueError):
        m.clip_retry_delay(0, base=0)
    with pytest.raises(ValueError):
        m.clip_retry_delay(0, max_delay=-1)


class BlockingWatermarkS3:
    """S3 double that wedges the watermark listing until released.

    Stands in for an S3 that accepts the connection and never answers, which is
    the state the running gauge must not report as working.
    """

    class _Meta:
        endpoint_url = "https://s3.fake.local"
        region_name = "fake-region-1"

    meta = _Meta()

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        self.entered.set()
        assert self.release.wait(10), "the watermark read was never released"
        yield {"Contents": []}

    def head_object(self, Bucket, Key):
        raise ClientError({"Error": {"Code": "404"}}, "HeadObject")


class ScriptedRunner:
    """Camera-task double scripted as (healthy_passes, error) per attempt.

    An attempt with no error marks health once, then holds the task open until
    cancelled - which is what a healthy run_camera does: it never returns.
    """

    def __init__(self, script):
        self.script = list(script)
        self.attempts = 0
        self.done = asyncio.Event()

    async def runner(self, on_pass_ok):
        self.attempts += 1
        healthy, error = self.script.pop(0) if self.script else (1, None)
        for _ in range(healthy):
            on_pass_ok()
        if error is not None:
            raise error
        self.done.set()
        await asyncio.Event().wait()


def run_supervisor_until(runner, camera, stop_after_sleeps):
    """Drive the supervisor until it has slept stop_after_sleeps times."""
    slept = []

    async def recording_sleep(seconds):
        slept.append(seconds)

    async def scenario():
        task = asyncio.create_task(
            m.supervise_camera(
                camera,
                runner.runner,
                backoff_seconds=5,
                backoff_max_seconds=300,
                sleep=recording_sleep,
            )
        )
        try:
            for _ in range(500):
                if len(slept) >= stop_after_sleeps or runner.done.is_set():
                    break
                await asyncio.sleep(0.005)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return slept

    return run(scenario())


def test_supervisor_restarts_a_failed_camera_and_reports_restarts():
    camera = "metric_cam_restart"
    runner = ScriptedRunner(
        [(0, requests.ConnectionError("frigate down")),
         (0, requests.Timeout("slow"))]
    )
    slept = run_supervisor_until(runner, camera, stop_after_sleeps=2)

    assert slept == [5, 10]
    assert sample("camera_restarts_total", camera=camera) == 2
    assert sample("errors_total", camera=camera, kind="frigate_unreachable") == 1
    assert sample("errors_total", camera=camera, kind="frigate_timeout") == 1


def test_task_running_is_1_only_after_the_watermark_resolves(frigate_server):
    """A task wedged in the S3 watermark lookup is not working yet."""
    camera = "metric_cam_running"
    s3 = BlockingWatermarkS3()
    client = m.make_frigate_client(frigate_server, username="", password="")

    async def runner(on_pass_ok):
        await m.run_camera(
            camera=camera,
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            watermark_lookback_seconds=3600,
            first_run_lookback_seconds=3600,
            idle_sleep_seconds=0.01,
            on_pass_ok=on_pass_ok,
        )

    async def scenario():
        task = asyncio.create_task(m.supervise_camera(camera, runner))
        # Give the task its turns, then read while it sits in the watermark read.
        await asyncio.sleep(0.05)
        assert s3.entered.wait(5), "the watermark lookup never started"
        wedged = observed("camera_task_running", camera=camera)

        s3.release.set()
        working = 0
        for _ in range(200):
            working = observed("camera_task_running", camera=camera)
            if working == 1:
                break
            await asyncio.sleep(0.01)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return wedged, working

    wedged, working = run(scenario())

    assert wedged == 0
    assert working == 1
    assert sample("camera_task_running", camera=camera) == 0


def test_the_supervisor_keeps_the_gauge_0_during_backoff():
    """The supervisor zeroes what a working camera task set to 1."""
    camera = "metric_cam_backoff"
    seen_while_working = []
    seen_during_backoff = []

    async def runner(on_pass_ok):
        # What a camera task does once it is working, per run_camera.
        metrics.CAMERA_TASK_RUNNING.labels(camera=camera).set(1)
        seen_while_working.append(sample("camera_task_running", camera=camera))
        raise requests.ConnectionError("frigate down")

    async def recording_sleep(seconds):
        seen_during_backoff.append(sample("camera_task_running", camera=camera))
        raise StopSupervisor

    async def scenario():
        task = asyncio.create_task(
            m.supervise_camera(
                camera, runner, backoff_seconds=5, sleep=recording_sleep
            )
        )
        await asyncio.gather(task, return_exceptions=True)

    run(scenario())

    assert seen_while_working == [1]
    assert seen_during_backoff == [0]
    assert sample("camera_task_running", camera=camera) == 0


def test_a_healthy_pass_resets_the_restart_backoff():
    camera = "metric_cam_reset"
    runner = ScriptedRunner(
        [(0, requests.Timeout("a")),
         (0, requests.Timeout("b")),
         (1, requests.Timeout("c"))]
    )
    # Third attempt marked a healthy pass before crashing, so its restart must
    # wait the base delay again rather than the third exponential step.
    slept = run_supervisor_until(runner, camera, stop_after_sleeps=3)

    assert slept == [5, 10, 5]


class StopSupervisor(BaseException):
    """Escapes the supervisor loop, which only guards against Exception."""


def test_a_task_that_returns_is_counted_as_a_failure():
    camera = "metric_cam_returned"

    async def runner(on_pass_ok):
        return None

    slept = []

    async def recording_sleep(seconds):
        slept.append(seconds)
        if len(slept) >= 2:
            raise StopSupervisor

    async def scenario():
        task = asyncio.create_task(
            m.supervise_camera(
                camera, runner, backoff_seconds=5, sleep=recording_sleep
            )
        )
        return await asyncio.gather(task, return_exceptions=True)

    results = run(scenario())

    assert isinstance(results[0], StopSupervisor)

    assert slept == [5, 10]
    assert sample("errors_total", camera=camera, kind="task_returned") == 2
    assert sample("camera_restarts_total", camera=camera) == 2
    assert sample("camera_task_running", camera=camera) == 0


def test_cancelling_the_supervisor_cancels_the_camera_task():
    camera = "metric_cam_cancel"
    cancelled = asyncio.Event()

    async def runner(on_pass_ok):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def scenario():
        task = asyncio.create_task(m.supervise_camera(camera, runner))
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    run(scenario())

    assert cancelled.is_set()
    assert sample("camera_task_running", camera=camera) == 0


def test_one_broken_camera_does_not_stop_another():
    broken = "metric_cam_broken"
    healthy = "metric_cam_healthy"
    archived = asyncio.Event()

    async def broken_runner(on_pass_ok):
        raise requests.ConnectionError("camera offline")

    async def healthy_runner(on_pass_ok):
        on_pass_ok()
        archived.set()
        await asyncio.Event().wait()

    async def yielding_sleep(seconds):
        await asyncio.sleep(0)

    async def scenario():
        failing = asyncio.create_task(
            m.supervise_camera(
                broken, broken_runner, backoff_seconds=1, sleep=yielding_sleep
            )
        )
        working = asyncio.create_task(
            m.supervise_camera(
                healthy, healthy_runner, backoff_seconds=1, sleep=yielding_sleep
            )
        )
        try:
            await asyncio.wait_for(archived.wait(), timeout=5)
            still_running = not failing.done()
        finally:
            failing.cancel()
            working.cancel()
            await asyncio.gather(failing, working, return_exceptions=True)
        return still_running

    assert run(scenario()) is True
    assert sample("camera_task_running", camera=healthy) == 0
