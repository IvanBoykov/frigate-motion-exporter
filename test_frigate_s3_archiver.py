import asyncio
import io
import json
import logging
import os
import signal
import socket
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zoneinfo import ZoneInfo

import pytest
import requests
from botocore.exceptions import EndpointConnectionError

import frigate_s3_archiver as m
import logging_setup
import metrics
from config import load_config


SEGMENT_SECONDS = 10
BASE_TS = 1_790_416_000
SEGMENTS_PER_CAMERA = 12
END_TS = BASE_TS + SEGMENTS_PER_CAMERA * SEGMENT_SECONDS

# cam_a: motion at segments 0-2, a gap, motion at 4, then a long quiet tail so
# the last interval ends far enough from the scan boundary to be archived.
CAM_A_MOTION = [1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0]
CAM_B_MOTION = [0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0]


def make_segments(motion_flags):
    segments = []
    for offset, has_motion in enumerate(motion_flags):
        start = BASE_TS + offset * SEGMENT_SECONDS
        segments.append(
            {
                "start_time": start,
                "end_time": start + SEGMENT_SECONDS,
                "motion": 1 if has_motion else 0,
            }
        )
    return segments


class FakeFrigateHandler(BaseHTTPRequestHandler):
    """Serves /api/config, /api/{cam}/recordings and clip.mp4 over real HTTP."""

    segments_by_camera = {}
    clip_failures = set()
    # First N requests to /api/config answer 503, standing in for a Frigate
    # (or its proxy) that is still starting while the archiver boots. 0 = never,
    # None = always.
    config_failures = 0
    config_requests = []
    requested_clips = []
    clip_attempts = []
    clip_raw_starts = []
    recording_requests = []
    clip_bytes = 1024
    # Clip responses for the first N attempts of a clip come back without the
    # mfra trailer, standing in for a killed export. 0 = never, None = always.
    truncate_clip_attempts = 0
    # Clip fetches answered with Frigate's "No recordings found" 400, standing
    # in for a window whose files are gone from disk. 0 = never, None = always.
    no_recordings_attempts = 0
    # Clip responses for the first N attempts come back as HTTP 200 with an
    # empty body, standing in for ffmpeg dying before its first packet while
    # the database still lists the recordings. 0 = never, None = always.
    empty_clip_attempts = 0
    # Clip starts answered with a non-Frigate 400 (an nginx HTML page), which
    # must never look like the no-recordings signature.
    reject_clips_400 = set()
    # Scripted per-attempt clip answers for a start: a list whose entries are
    # "truncate" / "empty" / "norecordings" / an HTTP status int. The last
    # entry repeats. Stands in for a fault that changes mid-chain (Frigate
    # dies, then the proxy answers 503 while it restarts).
    clip_sequences = {}
    # When set, a clip body is streamed in chunks with this delay between them,
    # so a clip download takes real time and can be interrupted.
    clip_stream_delay = 0.0
    clip_stream_chunk = 64 * 1024

    def log_message(self, *args):
        pass

    def _send(self, status, body=b"", content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        parts = path.split("/")

        if path.startswith("/api/config"):
            type(self).config_requests.append(path)
            remaining = type(self).config_failures
            if remaining is None or remaining != 0:
                if remaining is not None:
                    type(self).config_failures = remaining - 1
                self._send(503, b"starting")
                return
            cameras = {name: {} for name in self.segments_by_camera}
            self._send(200, json.dumps({"cameras": cameras}).encode())
            return

        if len(parts) >= 4 and parts[3] == "recordings":
            camera = parts[2]
            query = self.path.split("?")[-1] if "?" in self.path else ""
            self.recording_requests.append((camera, query))

            params = dict(
                item.split("=", 1) for item in query.split("&") if "=" in item
            )
            after = float(params.get("after", float("-inf")))
            before = float(params.get("before", float("inf")))

            # Frigate returns the segments overlapping the requested range.
            segments = [
                seg
                for seg in self.segments_by_camera.get(camera, [])
                if seg["end_time"] > after and seg["start_time"] < before
            ]
            self._send(200, json.dumps(segments).encode())
            return

        if parts[-1] == "clip.mp4":
            # Frigate declares the path parameters as float:
            # /{camera}/start/{start_ts}/end/{end_ts}/clip.mp4 - so any
            # float() parses, scientific notation included, and it still
            # exports a clip for the parsed (possibly shifted) range.
            start = float(parts[4])
            self.requested_clips.append(start)
            self.clip_attempts.append(start)
            self.clip_raw_starts.append(parts[4])

            if start in self.clip_failures:
                self._send(500, b"boom")
                return

            if start in self.reject_clips_400:
                self._send(400, b"<html><head>400 Bad Request</head></html>")
                return

            attempts_now = self.clip_attempts.count(start)

            sequence = self.clip_sequences.get(start)
            if sequence:
                action = sequence[min(attempts_now, len(sequence)) - 1]
                if isinstance(action, int):
                    self._send(action, b"proxy error")
                    return
                if action == "empty":
                    self._send(200, b"", content_type="video/mp4")
                    return
                if action == "norecordings":
                    self._send(
                        400,
                        json.dumps(
                            {
                                "success": False,
                                "message": (
                                    "No recordings found for the specified"
                                    " time range"
                                ),
                            }
                        ).encode(),
                    )
                    return
                if action == "truncate":
                    self._send(
                        200,
                        self.clip_body()[:-8],
                        content_type="video/mp4",
                    )
                    return

            if self.no_recordings_attempts is None or (
                attempts_now <= self.no_recordings_attempts
            ):
                self._send(
                    400,
                    json.dumps(
                        {
                            "success": False,
                            "message": (
                                "No recordings found for the specified"
                                " time range"
                            ),
                        }
                    ).encode(),
                )
                return

            if self.empty_clip_attempts is None or (
                attempts_now <= self.empty_clip_attempts
            ):
                # 200 headers are already committed before ffmpeg runs, so an
                # export that dies before its first packet reaches the client
                # as a successful response with no body.
                self._send(200, b"", content_type="video/mp4")
                return

            if self.clip_stream_delay:
                self._send_slow_clip()
                return

            body = self.clip_body()
            attempts = self.clip_attempts.count(start)
            if self.truncate_clip_attempts is None or (
                attempts <= self.truncate_clip_attempts
            ):
                # A killed export: the body stops inside a fragment, so the
                # mfra trailer never reaches the client.
                body = body[:-8]

            self._send(200, body, content_type="video/mp4")
            return

        self._send(404)

    @classmethod
    def clip_body(cls):
        """A body of at least ``clip_bytes`` ending with an mfra trailer.

        Byte layout matches what the deployed ffmpeg writes: mfra is a plain
        container (8-byte header, no version/flags) holding tfra (24 bytes,
        empty here) and mfro (16 bytes with its version/flags field).
        """
        tfra = struct.pack(">I4s4sIII", 24, b"tfra", b"\x01\x00\x00\x00", 1, 0, 0)
        mfro = struct.pack(
            ">I4s4sI", 16, b"mfro", b"\x00\x00\x00\x00", 8 + len(tfra)
        )
        mfra = (
            struct.pack(">I4s", 8 + len(tfra) + len(mfro), b"mfra")
            + tfra
            + mfro
        )
        ftyp = struct.pack(
            ">I4s4sI4s", 24, b"ftyp", b"isom", 0x200, b"isom"
        )
        filler = b"y" * max(0, cls.clip_bytes - len(ftyp) - len(mfra))
        return ftyp + filler + mfra

    def _send_slow_clip(self):
        """Stream clip_bytes in chunks, sleeping between them.

        Stands in for a Frigate that is exporting a long clip: the body keeps
        arriving for as long as the client keeps reading it.
        """
        body = self.clip_body()
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

        sent = 0
        try:
            while sent < len(body):
                size = min(self.clip_stream_chunk, len(body) - sent)
                self.wfile.write(body[sent:sent + size])
                self.wfile.flush()
                sent += size
                time.sleep(self.clip_stream_delay)
        except (BrokenPipeError, ConnectionResetError):
            pass


class FakeS3Client:
    """Records the objects that would have been written, in call order."""

    class _Meta:
        class events:
            @staticmethod
            def register(event_name, handler):
                pass

        endpoint_url = "https://s3.fake.local"
        region_name = "fake-region-1"

    meta = _Meta()

    def __init__(self, existing_keys=()):
        self.existing_keys = set(existing_keys)
        self.puts = []
        self.parts = []
        self.aborted = []
        self.completed = []
        self.reject = False
        self.calls_after_reject = []
        self._upload_ids = 0

    def _note(self, call):
        # Set ``reject`` once the async level reports the task finished: any
        # call after that comes from a worker thread that kept working.
        # Recorded rather than raised, so that a late call cannot turn a race
        # inside the test into an exception in the very thread under test.
        if self.reject:
            self.calls_after_reject.append(call)

    def head_object(self, Bucket, Key):
        self._note("head_object")
        if Key in self.existing_keys:
            return {"ContentLength": 1024}

        error = {"Error": {"Code": "404"}}
        raise m.ClientError(error, "HeadObject")

    def put_object(self, Bucket, Key, Body, ContentType):
        self._note("put_object")
        self.puts.append((Key, len(Body)))

    def create_multipart_upload(self, Bucket, Key, ContentType):
        self._note("create_multipart_upload")
        self._upload_ids += 1
        return {"UploadId": f"upload-{self._upload_ids}"}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self._note(f"upload_part:{PartNumber}")
        self.parts.append((Key, PartNumber, len(Body)))
        return {"ETag": f'"{PartNumber}"'}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        self._note("complete_multipart_upload")
        self.completed.append((Key, UploadId))
        return {"ETag": '"done"'}

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.append(UploadId)

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        self._note("get_paginator")
        return FakeListPaginator(self)


class FakeListPaginator:
    def __init__(self, s3):
        self.s3 = s3

    def paginate(self, Bucket, Prefix):
        keys = sorted(
            key
            for key in set(self.s3.existing_keys)
            | {key for key, _ in self.s3.puts}
            if key.startswith(Prefix)
        )
        yield {"Contents": [{"Key": key} for key in keys]}


SCAN_OPTIONS = {
    "chunk_seconds": 3600,
    "interval_chunk_seconds": 3600,
}


@pytest.fixture()
def frigate_server():
    FakeFrigateHandler.segments_by_camera = {
        "cam_a": make_segments(CAM_A_MOTION),
        "cam_b": make_segments(CAM_B_MOTION),
    }
    FakeFrigateHandler.clip_failures = set()
    FakeFrigateHandler.config_failures = 0
    FakeFrigateHandler.config_requests = []
    FakeFrigateHandler.requested_clips = []
    FakeFrigateHandler.clip_attempts = []
    FakeFrigateHandler.clip_raw_starts = []
    FakeFrigateHandler.truncate_clip_attempts = 0
    FakeFrigateHandler.no_recordings_attempts = 0
    FakeFrigateHandler.empty_clip_attempts = 0
    FakeFrigateHandler.reject_clips_400 = set()
    FakeFrigateHandler.clip_sequences = {}
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


def run(coroutine):
    return asyncio.run(coroutine)


def scan(client, camera, after_ts=float(BASE_TS), **overrides):
    return run(ascan(client, camera, after_ts, **overrides))


async def ascan(client, camera, after_ts=float(BASE_TS), **overrides):
    options = {**SCAN_OPTIONS, **overrides}
    return await m.find_motion_intervals(
        camera,
        after_ts,
        float(END_TS),
        frigate_client=client,
        **options,
    )


def make_client(frigate_server, **auth):
    auth.setdefault("username", "")
    auth.setdefault("password", "")
    return m.make_frigate_client(frigate_server, **auth)


def test_client_without_credentials_is_unauthenticated(frigate_server):
    client = make_client(frigate_server)
    assert client["session"].auth is None
    assert run(m.fetch_cameras(client)) == ["cam_a", "cam_b"]


def test_client_with_credentials_reads_the_same_api(frigate_server):
    client = m.make_frigate_client(
        frigate_server, username="user", password="secret"
    )
    assert client["session"].auth is not None
    assert run(m.fetch_cameras(client)) == ["cam_a", "cam_b"]


def test_rejects_non_http_base_url():
    with pytest.raises(ValueError):
        m.make_frigate_client("not-a-url")


class RecordingSleep:
    """An asyncio.sleep stand-in that records its delays and never waits."""

    def __init__(self):
        self.delays = []

    async def __call__(self, seconds):
        self.delays.append(seconds)


def test_camera_discovery_survives_a_brief_frigate_outage(frigate_server):
    """A Frigate still booting at startup is retried, not fatal.

    The first two /api/config answers are 503; discovery must keep retrying
    and return the camera list once Frigate answers.
    """
    FakeFrigateHandler.config_failures = 2
    slept = RecordingSleep()

    cameras = run(
        m.fetch_cameras_with_retry(
            make_client(frigate_server), sleep=slept
        )
    )

    assert cameras == ["cam_a", "cam_b"]
    # Two failures, two waits, no wait after the success.
    assert slept.delays == [
        m.DEFAULT_CAMERA_DISCOVERY_RETRY_SECONDS,
        m.DEFAULT_CAMERA_DISCOVERY_RETRY_SECONDS,
    ]


def test_camera_discovery_gives_up_after_its_budget(frigate_server):
    """After the budget the last error is raised: startup fails, no hang."""

    FakeFrigateHandler.config_failures = None
    slept = RecordingSleep()
    attempts = 3

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        run(
            m.fetch_cameras_with_retry(
                make_client(frigate_server),
                attempts=attempts,
                sleep=slept,
            )
        )

    assert excinfo.value.response.status_code == 503
    # Every failed attempt slept once, the last one raised instead.
    assert len(slept.delays) == attempts - 1
    assert len(FakeFrigateHandler.config_requests) == attempts


def test_camera_discovery_defaults_match_the_documented_budget():
    """The startup budget the README promises is the one the code uses."""

    assert m.DEFAULT_CAMERA_DISCOVERY_ATTEMPTS == 12
    assert m.DEFAULT_CAMERA_DISCOVERY_RETRY_SECONDS == 5.0


def test_camera_discovery_rejects_an_empty_budget():
    with pytest.raises(ValueError):
        run(m.fetch_cameras_with_retry(None, attempts=0))


def test_find_motion_intervals_per_camera(frigate_server):
    client = make_client(frigate_server)

    assert scan(client, "cam_a") == [
        (float(BASE_TS), float(BASE_TS + 3 * SEGMENT_SECONDS)),
        (float(BASE_TS + 4 * SEGMENT_SECONDS), float(BASE_TS + 5 * SEGMENT_SECONDS)),
    ]
    assert scan(client, "cam_b") == [
        (float(BASE_TS + SEGMENT_SECONDS), float(BASE_TS + 4 * SEGMENT_SECONDS)),
    ]


def test_clip_url_sends_integer_seconds_widened_outward(frigate_server):
    """The clip URL carries floor(start)/ceil(end) as plain integers.

    Frigate parses the path parameters as float, so scientific notation is
    not rejected - "%g" silently shifts the value by up to 500 s and merges
    the window ends, and Frigate exports the wrong window without noticing.
    Integers cannot shift, and widening outward keeps every segment the scan
    reported inside the requested window.
    """
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=float(BASE_TS) + 0.4,
            end_ts=float(BASE_TS + SEGMENT_SECONDS) + 0.4,
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )

    assert FakeFrigateHandler.requested_clips == [float(BASE_TS)]
    assert FakeFrigateHandler.clip_raw_starts == [str(BASE_TS)]
    assert s3.puts[0][1] == 1024
    # the key still names the interval, not the widened request
    assert s3.puts[0][0] == m.make_s3_key("cam_a", float(BASE_TS) + 0.4)


def test_upload_intervals_is_chronological_even_if_input_is_not(frigate_server):
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    shuffled = [
        (float(BASE_TS + 4 * SEGMENT_SECONDS), float(BASE_TS + 5 * SEGMENT_SECONDS)),
        (float(BASE_TS + 2 * SEGMENT_SECONDS), float(BASE_TS + 3 * SEGMENT_SECONDS)),
        (float(BASE_TS), float(BASE_TS + SEGMENT_SECONDS)),
    ]

    uploaded, skipped, last_end, _truncated = run(
        m.upload_intervals(
            intervals=shuffled,
            camera="cam_a",
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )

    starts = [m.parse_s3_clip_start("cam_a", key) for key, _ in s3.puts]
    assert starts == [
        BASE_TS,
        BASE_TS + 2 * SEGMENT_SECONDS,
        BASE_TS + 4 * SEGMENT_SECONDS,
    ]
    assert (uploaded, skipped) == (3, 0)
    assert last_end == BASE_TS + 5 * SEGMENT_SECONDS


def test_upload_failure_aborts_before_later_clips(frigate_server):
    """A failed clip must stop the pass, not get skipped over."""
    client = make_client(frigate_server)
    first_start = float(BASE_TS + SEGMENT_SECONDS)
    FakeFrigateHandler.clip_failures = {first_start}

    s3 = FakeS3Client()
    intervals = [
        (first_start, first_start + SEGMENT_SECONDS),
        (float(BASE_TS + 3 * SEGMENT_SECONDS), float(BASE_TS + 4 * SEGMENT_SECONDS)),
    ]

    with pytest.raises(m.requests.exceptions.HTTPError):
        run(
            m.upload_intervals(
                intervals=intervals,
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
            )
        )

    # Nothing may land in the bucket ahead of the missing clip.
    assert s3.puts == []
    assert FakeFrigateHandler.requested_clips == [first_start]


def test_existing_clip_is_skipped_without_refetch(frigate_server):
    client = make_client(frigate_server)
    key = m.make_s3_key("cam_a", BASE_TS)
    s3 = FakeS3Client(existing_keys=[key])

    uploaded, skipped, last_end, _truncated = run(
        m.upload_intervals(
            intervals=[(float(BASE_TS), float(BASE_TS + SEGMENT_SECONDS))],
            camera="cam_a",
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )

    assert (uploaded, skipped) == (0, 1)
    assert FakeFrigateHandler.requested_clips == []
    assert last_end == BASE_TS + SEGMENT_SECONDS


def test_rescan_after_a_pass_finds_nothing_new(frigate_server):
    """The watermark a pass hands back must not re-report finished intervals."""
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    async def scenario():
        intervals = await ascan(client, "cam_a")
        assert len(intervals) == 2

        uploaded, skipped, last_end, _truncated = await m.upload_intervals(
            intervals=intervals,
            camera="cam_a",
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
        next_watermark = m.next_after_ts(last_end)
        return uploaded, await ascan(
            client, "cam_a", after_ts=next_watermark
        )

    uploaded, remaining = run(scenario())

    assert uploaded == 2
    assert remaining == []


class StopLoop(Exception):
    """Raised by an exhausted clock to end the endless per-camera loop."""


class ScriptedClock:
    """Return `value` for `calls` iterations, then stop the camera loop."""

    def __init__(self, value, calls):
        self.value = value
        self.remaining = calls

    def __call__(self):
        if self.remaining <= 0:
            raise StopLoop
        self.remaining -= 1
        return self.value


def run_camera_options():
    """Make BASE_TS the first-run start and keep the loop deterministic."""
    return {
        "watermark_lookback_seconds": END_TS + 3600 - BASE_TS,
        "first_run_lookback_seconds": END_TS - BASE_TS,
        "idle_sleep_seconds": 0.01,
    }


def test_intervals_found_gauge_tracks_each_search(frigate_server, monkeypatch):
    """The gauge holds the count this pass's search returned, including 0."""
    client = make_client(frigate_server)
    s3 = FakeS3Client()
    seen = []
    real_upload_intervals = m.upload_intervals

    async def spy_upload_intervals(intervals, **kwargs):
        seen.append(
            metrics.CAMERA_MOTION_INTERVALS_FOUND.labels(camera="cam_a")._value.get()
        )
        return await real_upload_intervals(intervals, **kwargs)

    monkeypatch.setattr(m, "upload_intervals", spy_upload_intervals)

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    # cam_a has two motion intervals in the scan window, and the pass that
    # archived them reported exactly that.
    assert seen == [2]
    # The follow-up pass found nothing, and the gauge reads the latest search,
    # not the last one that found something.
    assert metrics.CAMERA_MOTION_INTERVALS_FOUND.labels(camera="cam_a")._value.get() == 0


def test_intervals_found_gauge_is_zero_without_motion(frigate_server):
    """A search that returns nothing reads 0, not a stale count."""
    client = make_client(frigate_server)
    s3 = FakeS3Client()
    FakeFrigateHandler.segments_by_camera["cam_a"] = make_segments([0] * SEGMENTS_PER_CAMERA)
    metrics.CAMERA_MOTION_INTERVALS_FOUND.labels(camera="cam_a").set(7)

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    assert s3.puts == []
    assert metrics.CAMERA_MOTION_INTERVALS_FOUND.labels(camera="cam_a")._value.get() == 0


def test_run_camera_uploads_then_rescans_from_new_watermark(frigate_server):
    """One pass archives everything, the next pass finds nothing new."""
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    starts = [m.parse_s3_clip_start("cam_a", key) for key, _ in s3.puts]
    assert starts == [BASE_TS, BASE_TS + 4 * SEGMENT_SECONDS]
    assert FakeFrigateHandler.requested_clips == [
        float(BASE_TS),
        float(BASE_TS + 4 * SEGMENT_SECONDS),
    ]

    # The rescan started after the end of the first pass, so finished
    # intervals were neither re-uploaded nor re-reported.
    last_query = FakeFrigateHandler.recording_requests[-1][1]
    last_after = float(last_query.split("after=")[1].split("&")[0])
    assert last_after >= BASE_TS + 5 * SEGMENT_SECONDS


def test_run_camera_stops_on_first_unrecoverable_upload(frigate_server):
    client = make_client(frigate_server)
    FakeFrigateHandler.clip_failures = {float(BASE_TS)}
    s3 = FakeS3Client()

    with pytest.raises(m.requests.exceptions.HTTPError):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    assert s3.puts == []
    assert FakeFrigateHandler.requested_clips == [float(BASE_TS)]


def test_run_camera_resumes_from_bucket_contents(frigate_server):
    """The newest clip already in the bucket sets where scanning starts."""
    client = make_client(frigate_server)
    newest = BASE_TS + 4 * SEGMENT_SECONDS
    s3 = FakeS3Client(existing_keys=[m.make_s3_key("cam_a", newest)])

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    assert FakeFrigateHandler.recording_requests, "no scan was issued"
    first_query = FakeFrigateHandler.recording_requests[0][1]
    first_after = float(first_query.split("after=")[1].split("&")[0])
    assert first_after >= newest

    # The only interval the scan could still report is the archived one, and
    # it is not fetched from Frigate or written again.
    assert s3.puts == []
    assert FakeFrigateHandler.requested_clips == []


def test_skip_advances_the_watermark_past_still_visible_segments(frigate_server):
    """Review finding #5: an existing object must restore the watermark, not pin it.

    The bucket holds the clip starting at the first motion interval; Frigate
    still reports its segments plus fresh motion after them. The existing clip
    is skipped without a fetch, but the pass must advance the watermark to the
    end of the *whole* scan range - the next query has to start after the fresh
    segments, or a bucket that keeps "proving" old clips freezes progress.
    """
    client = make_client(frigate_server)
    # cam_a motion: BASE_TS..BASE_TS+50 (5 segments). Fresh motion follows at
    # BASE_TS+70..BASE_TS+90, closed by a motion=0 segment before the scan
    # boundary, so the scan sees two settled intervals.
    FakeFrigateHandler.segments_by_camera["cam_a"] = make_segments(
        [1, 1, 1, 1, 1, 0, 0, 1, 1, 0]
    )
    archived_start = float(BASE_TS)
    s3 = FakeS3Client(existing_keys=[m.make_s3_key("cam_a", archived_start)])

    # The scan boundary sits 90 s past the last motion=0 segment, well outside
    # the tail-defer tolerance, so both intervals are settled.
    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS + 60, calls=3),
                **run_camera_options(),
            )
        )

    # The archived interval was skipped without re-fetch; the fresh one was
    # archived normally.
    assert FakeFrigateHandler.requested_clips == [float(BASE_TS + 70)]
    assert [key for key, _ in s3.puts] == [m.make_s3_key("cam_a", float(BASE_TS + 70))]

    # And the watermark moved past everything the scan saw: the follow-up query
    # starts at the end of the fresh interval (+ margin), not at the stored clip.
    queries = [
        float(q.split("after=")[1].split("&")[0])
        for _, q in FakeFrigateHandler.recording_requests
    ]
    assert len(queries) >= 2
    assert queries[-1] >= float(BASE_TS + 90)


def test_all_cameras_are_processed_concurrently(frigate_server, monkeypatch):
    """async_main starts one supervised task per camera the API reports."""
    started = {}

    async def fake_run_camera(camera, **kwargs):
        started[camera] = kwargs["s3_bucket"]
        clocks[camera] = kwargs["now_fn"]
        await asyncio.Event().wait()

    clocks = {}
    clock = lambda: END_TS

    async def scenario(cfg):
        task = asyncio.create_task(
            m.async_main(config=cfg, now_fn=clock)
        )
        for _ in range(200):
            if len(started) == 2:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    monkeypatch.setenv("FRIGATE_URL", frigate_server)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("FRIGATE_USER", "")
    monkeypatch.setenv("FRIGATE_PASSWORD", "")
    monkeypatch.setattr(
        m.metrics, "start_metrics_server", lambda port, bind=None: port
    )
    monkeypatch.setattr(m.boto3, "client", lambda name, **kw: FakeS3Client())
    monkeypatch.setattr(m, "run_camera", fake_run_camera)

    cfg = load_config()
    run(scenario(cfg))

    assert started == {"cam_a": "bucket", "cam_b": "bucket"}
    # The injected clock must reach every camera task, not vanish in main.
    assert clocks == {"cam_a": clock, "cam_b": clock}


TEN_MINUTE_CHUNK_SECONDS = 600
LONG_MOTION_SEGMENTS = 150  # 25 minutes of continuous motion


def scan_window(frigate_server, flags, scan_segments, camera="cam_a"):
    """Scan `scan_segments` * SEGMENT_SECONDS of a camera seeded with `flags`."""
    client = make_client(frigate_server)
    FakeFrigateHandler.segments_by_camera[camera] = make_segments(flags)

    return run(
        m.find_motion_intervals(
            camera,
            float(BASE_TS),
            float(BASE_TS + scan_segments * SEGMENT_SECONDS),
            frigate_client=client,
            chunk_seconds=3600,
            interval_chunk_seconds=TEN_MINUTE_CHUNK_SECONDS,
        )
    )


def test_long_motion_is_split_into_ten_minute_chunks(frigate_server):
    """25 min of motion, long since ended -> two full chunks plus a tail."""
    intervals = scan_window(
        frigate_server,
        [1] * LONG_MOTION_SEGMENTS + [0] * 20,
        scan_segments=LONG_MOTION_SEGMENTS + 20,
    )

    assert intervals == [
        (float(BASE_TS), float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS)),
        (float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS), float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS)),
        (
            float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS),
            float(BASE_TS + LONG_MOTION_SEGMENTS * SEGMENT_SECONDS),
        ),
    ]


def test_short_tail_near_the_scan_boundary_is_deferred(frigate_server):
    """The event stopped just before the boundary, so it may still be running.

    Its partial tail is left for a later pass instead of being archived as a
    short clip; the full ten minute chunks are unaffected.
    """
    intervals = scan_window(
        frigate_server,
        [1] * LONG_MOTION_SEGMENTS + [0] * 2,
        scan_segments=LONG_MOTION_SEGMENTS + 2,
    )

    assert intervals == [
        (float(BASE_TS), float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS)),
        (float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS), float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS)),
    ]


def test_tail_past_the_tolerance_is_archived(frigate_server):
    """50 s of quiet - over the 40 s tolerance - proves the event has ended."""
    intervals = scan_window(
        frigate_server,
        [1] * LONG_MOTION_SEGMENTS + [0] * 5,
        scan_segments=LONG_MOTION_SEGMENTS + 5,
    )

    assert len(intervals) == 3
    assert intervals[-1] == (
        float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS),
        float(BASE_TS + LONG_MOTION_SEGMENTS * SEGMENT_SECONDS),
    )


def test_motion_still_on_at_the_boundary_yields_no_partial_clip(frigate_server):
    """With motion open at the boundary only full chunks are archived."""
    intervals = scan_window(
        frigate_server,
        [1] * LONG_MOTION_SEGMENTS,
        scan_segments=LONG_MOTION_SEGMENTS,
    )

    assert intervals == [
        (float(BASE_TS), float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS)),
        (float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS), float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS)),
    ]


def scan_motion_only(frigate_server, motion_flags, scan_segments, camera="cam_a"):
    """Scan a database shaped like a motion-retention camera.

    Such a camera stores no motion=0 rows at all (motion-mode retention
    discards them), so silence is the absence of rows: nothing in the data
    can ever close an interval explicitly.
    """
    client = make_client(frigate_server)
    FakeFrigateHandler.segments_by_camera[camera] = [
        seg for seg in make_segments(motion_flags) if seg["motion"]
    ]

    return run(
        m.find_motion_intervals(
            camera,
            float(BASE_TS),
            float(BASE_TS + scan_segments * SEGMENT_SECONDS),
            frigate_client=client,
            chunk_seconds=3600,
            interval_chunk_seconds=TEN_MINUTE_CHUNK_SECONDS,
        )
    )


def test_a_motion_only_interval_ends_at_its_last_segment(frigate_server):
    """Without motion=0 rows the interval ends where recording stopped.

    The scan boundary must never stand in for the interval end: after a
    downtime the scan reaches far past the event, and an end invented at
    the boundary turns a 5-minute event into fake 10-minute chunks of
    empty time.
    """
    intervals = scan_motion_only(frigate_server, [1] * 30, scan_segments=120)

    assert intervals == [
        (float(BASE_TS), float(BASE_TS + 30 * SEGMENT_SECONDS))
    ]


def test_a_settled_short_event_is_archived_without_a_full_chunk(
    frigate_server,
):
    """Silence past MAX_GAP settles a short event early.

    The tail rule holds back an interval only while it ends closer than
    MAX_GAP to the scan boundary, so on a motion-only database a 2-minute
    event archives in full as soon as the recorder has been silent for
    MAX_GAP - it does not wait for the scan to grow past start + a chunk,
    and its end is the last segment, not the boundary.
    """
    intervals = scan_motion_only(frigate_server, [1] * 12, scan_segments=22)

    assert intervals == [(float(BASE_TS), float(BASE_TS + 120))]


def test_a_running_motion_only_event_still_defers_its_tail(frigate_server):
    """Ongoing events keep streaming full chunks with the tail held back."""
    intervals = scan_motion_only(
        frigate_server,
        [1] * LONG_MOTION_SEGMENTS,
        scan_segments=LONG_MOTION_SEGMENTS + 1,
    )

    assert intervals == [
        (float(BASE_TS), float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS)),
        (float(BASE_TS + TEN_MINUTE_CHUNK_SECONDS), float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS)),
    ]


def test_only_the_last_interval_can_have_its_tail_deferred(frigate_server):
    """A short interval away from the boundary is archived in full."""
    flags = (
        [1] * 12
        + [0] * (LONG_MOTION_SEGMENTS - 12)
        + [1] * 12
    )
    intervals = scan_window(frigate_server, flags, scan_segments=len(flags))

    # The first 2-minute event is nowhere near the boundary, so it is kept;
    # the second one runs into the boundary and defers its tail.
    assert intervals == [(float(BASE_TS), float(BASE_TS + 12 * SEGMENT_SECONDS))]


def test_deferred_tail_is_archived_by_a_later_pass(frigate_server):
    """The same data scanned later archives what was previously deferred."""
    flags = [1] * LONG_MOTION_SEGMENTS + [0] * 20

    ongoing = scan_window(
        frigate_server, flags, scan_segments=LONG_MOTION_SEGMENTS + 2
    )
    settled = scan_window(
        frigate_server, flags, scan_segments=LONG_MOTION_SEGMENTS + 20
    )

    assert len(ongoing) == 2
    assert len(settled) == 3
    assert settled[-1] == (
        float(BASE_TS + 2 * TEN_MINUTE_CHUNK_SECONDS),
        float(BASE_TS + LONG_MOTION_SEGMENTS * SEGMENT_SECONDS),
    )


def reviewer_segments():
    """61 motion segments T..T+610, then quiet until T+660."""
    return make_segments([1] * 61 + [0] * 5)


def test_straddling_segment_survives_the_query_margin(frigate_server):
    """The margin may widen the query, never the logical bound.

    Pass 1 ends at T+612: the full chunk T..T+600 is archived, the tail
    T+600..T+610 is deferred because it ends 2 s from the scan boundary.
    Pass 2 queries Frigate with after=T+602, Frigate re-returns the
    straddling segment T+600..T+610, and min_start_ts must still sit exactly
    on T+600 - with the margin baked into it the filter drops that segment
    and the tail is lost forever.
    """
    client = make_client(frigate_server)
    FakeFrigateHandler.segments_by_camera["cam_a"] = reviewer_segments()

    def scan_pass(after_ts, min_start_ts, end_ts):
        return run(
            m.find_motion_intervals(
                "cam_a",
                float(after_ts),
                float(end_ts),
                frigate_client=client,
                chunk_seconds=3600,
                interval_chunk_seconds=600,
                min_start_ts=float(min_start_ts),
            )
        )

    first = scan_pass(BASE_TS, BASE_TS, BASE_TS + 612)
    assert first == [(float(BASE_TS), float(BASE_TS + 600))]

    watermark = first[-1][1]
    assert watermark == float(BASE_TS + 600)

    second = scan_pass(m.next_after_ts(watermark), watermark, BASE_TS + 660)
    assert second == [(float(BASE_TS + 600), float(BASE_TS + 610))]

    # Pin the regression itself: passing the margin as the logical bound
    # drops the straddling segment and the tail disappears.
    lost = scan_pass(
        m.next_after_ts(watermark),
        m.next_after_ts(watermark),
        BASE_TS + 660,
    )
    assert lost == []


class SequencedClock:
    """Return scripted timestamps, then stop the camera loop."""

    def __init__(self, values):
        self.values = list(values)

    def __call__(self):
        if not self.values:
            raise StopLoop
        return self.values.pop(0)


def test_run_camera_archives_deferred_tail_on_next_pass(frigate_server):
    """End-to-end: pass 1 chunks, pass 2 archives the settled tail."""
    client = make_client(frigate_server)
    FakeFrigateHandler.segments_by_camera["cam_a"] = reviewer_segments()
    s3 = FakeS3Client()

    # resolve: T+612 (so the empty-bucket start is T), pass 1: T+612,
    # pass 2: T+660, then the loop is stopped.
    clock = SequencedClock([BASE_TS + 612, BASE_TS + 612, BASE_TS + 660])

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                watermark_lookback_seconds=3600,
                first_run_lookback_seconds=612,
                idle_sleep_seconds=0.01,
                now_fn=clock,
            )
        )

    starts = [m.parse_s3_clip_start("cam_a", key) for key, _ in s3.puts]
    assert starts == [float(BASE_TS), float(BASE_TS + 600)]
    assert FakeFrigateHandler.requested_clips == [
        float(BASE_TS),
        float(BASE_TS + 600),
    ]


def test_s3_client_carries_the_retry_policy():
    conf = m.make_s3_client().meta.config
    assert conf.retries["total_max_attempts"] == (
        m.DEFAULT_S3_TOTAL_MAX_ATTEMPTS
    )
    assert conf.retries["mode"] == m.DEFAULT_S3_RETRY_MODE
    assert conf.read_timeout == m.DEFAULT_S3_READ_TIMEOUT_SECONDS
    assert conf.connect_timeout == m.DEFAULT_S3_CONNECT_TIMEOUT_SECONDS


def test_s3_client_stays_on_the_default_credential_chain(monkeypatch):
    # Nothing but the time budget may be pinned in code: credentials, region
    # and endpoint must keep coming from the standard boto3 chain.
    seen = {}
    registered = []

    class FakeEvents:
        def register(self, event_name, handler):
            registered.append((event_name, handler))

    class FakeClient:
        def __init__(self):
            self.meta = type("Meta", (), {"events": FakeEvents()})()

    def fake_client(name, **kw):
        seen["name"] = name
        seen["kw"] = kw
        return FakeClient()

    monkeypatch.setattr(m.boto3, "client", fake_client)
    m.make_s3_client()

    assert seen["name"] == "s3"
    assert set(seen["kw"]) == {"config"}
    # SDK-internal retries become our own DEBUG lines through this hook.
    assert registered == [("needs-retry", m._log_boto_retry)]


def test_s3_client_rejects_a_non_positive_time_budget():
    with pytest.raises(ValueError):
        m.make_s3_client(read_timeout_seconds=0)
    with pytest.raises(ValueError):
        m.make_s3_client(connect_timeout_seconds=-1)
    with pytest.raises(ValueError):
        m.make_s3_client(total_max_attempts=0)


def test_upload_bodies_are_rewindable(frigate_server):
    """Retrying an upload depends on botocore rewinding the request body.

    A generator or socket-backed body fails on the first 5xx without a single
    retry, so every body handed to S3 must be a rewindable object.
    """
    client = make_client(frigate_server)
    bodies = []

    class RecordingS3(FakeS3Client):
        def put_object(self, Bucket, Key, Body, ContentType):
            bodies.append(Body)
            super().put_object(Bucket, Key, Body, ContentType)

    s3 = RecordingS3()
    run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=float(BASE_TS),
            end_ts=float(BASE_TS + SEGMENT_SECONDS),
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )

    assert len(bodies) == 1
    assert isinstance(bodies[0], (bytes, bytearray, io.IOBase))


def test_multipart_cancellation_stops_at_the_part_boundary(frigate_server):
    """A cancel raised mid-multipart must stop the loop and abort the upload.

    The async test below cannot check this deterministically: with a real
    network and a real part size, a cancellation that lands between two parts
    still completes the clip before the next checkpoint. Here the stream is a
    local generator, so every boundary is reachable in one thread.
    """
    cancel_event = threading.Event()
    parts_seen = []

    class MultipartS3(FakeS3Client):
        def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
            parts_seen.append(PartNumber)
            # The caller asks for cancellation while this part is in flight.
            cancel_event.set()
            return super().upload_part(Bucket, Key, UploadId, PartNumber, Body)

    s3 = MultipartS3()
    stream = iter([b"y" * (4 * 1024 * 1024)] * 4)

    class StreamingResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=None):
            return stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class StreamingSession:
        def get(self, url, stream=False, timeout=None):
            return StreamingResponse()

    with pytest.raises(asyncio.CancelledError):
        m._upload_clip_hybrid_sync(
            camera="cam_a",
            start_ts=float(BASE_TS),
            end_ts=float(BASE_TS + 10 * SEGMENT_SECONDS),
            frigate_client={
                "session": StreamingSession(),
                "base_url": "http://fake",
            },
            s3_bucket="bucket",
            s3_client=s3,
            key_timezone=m.DEFAULT_S3_KEY_TIMEZONE,
            part_size=m.DEFAULT_PART_SIZE,
            http_chunk_size=1024 * 1024,
            http_timeout=30,
            cancel_event=cancel_event,
        )

    # One part was in flight when cancellation was requested; the loop must not
    # have uploaded another one, and the partial upload must be aborted.
    assert parts_seen == [1]
    assert s3.aborted == ["upload-1"]
    assert s3.completed == []


def test_cancelling_an_upload_stops_the_worker_thread(frigate_server):
    """The physical S3 work must stop when the task is cancelled.

    ``to_thread`` cannot cancel a running thread: awaiting its future again is a
    no-op and ``gather`` returns as soon as the task is cancelled, so a test that
    only asserts the task ended proves nothing about the work. What is checked
    here is whether the thread still touches S3 after the task has been reported
    finished.
    """
    FakeFrigateHandler.clip_bytes = 8 * 1024 * 1024
    FakeFrigateHandler.clip_stream_delay = 0.02
    FakeFrigateHandler.clip_stream_chunk = 64 * 1024

    client = make_client(frigate_server)
    s3 = FakeS3Client()

    async def scenario():
        task = asyncio.create_task(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=float(BASE_TS),
                end_ts=float(BASE_TS + 4 * SEGMENT_SECONDS),
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
            )
        )
        # Cancel only once the multipart upload exists, ie while the thread is
        # genuinely mid-upload rather than before it started.
        for _ in range(400):
            if s3.parts:
                break
            await asyncio.sleep(0.005)
        assert s3.parts, "the upload never reached upload_part"

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        s3.reject = True
        await asyncio.sleep(0.5)

    run(scenario())

    assert s3.aborted == ["upload-1"], "the partial upload was not aborted"
    assert s3.completed == [], "a cancelled clip was still completed"
    assert s3.puts == []
    assert s3.calls_after_reject == []


def test_part_size_below_the_multipart_minimum_is_rejected():
    """S3 rejects non-final parts under 5 MiB, and only at completion time."""
    with pytest.raises(ValueError):
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=float(BASE_TS),
                end_ts=float(BASE_TS + SEGMENT_SECONDS),
                frigate_client={"session": None, "base_url": "http://x"},
                s3_bucket="bucket",
                s3_client=FakeS3Client(),
                part_size=1024 * 1024,
            )
        )


def test_the_multipart_minimum_accepts_the_default_part_size():
    assert m.DEFAULT_PART_SIZE >= m.MIN_MULTIPART_PART_SIZE


# Bucket-state recovery matrix. Every state below is a state the bucket can be
# left in by a normal exit or by SIGKILL at some point of a pass; each test
# pins what a restart must do about it, so a change in the watermark or key
# scheme cannot silently turn one of these states into lost footage.
def test_restart_after_a_kill_mid_pass_reuploads_the_missing_clip(frigate_server):
    """Killed after chunk 1 of a pass: the bucket holds one clip, coverage is
    only until T+600, and the restart must recover exactly the missing rest.

    In-process the watermark was already T+600 when the kill happened, but the
    bucket only proves coverage up to the start of the newest clip. The restart
    therefore resumes one clip early - that regression is what makes the mid-pass
    kill safe: the scan re-derives the settled tail, head_object skips the
    archived chunk, and only the missing bytes are fetched.
    """
    client = make_client(frigate_server)
    FakeFrigateHandler.segments_by_camera["cam_a"] = reviewer_segments()
    s3 = FakeS3Client(existing_keys=[m.make_s3_key("cam_a", float(BASE_TS))])

    # resolve: T+660, pass: T+660, then the loop is stopped.
    clock = SequencedClock([BASE_TS + 660, BASE_TS + 660])

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                watermark_lookback_seconds=3600,
                first_run_lookback_seconds=612,
                idle_sleep_seconds=0.01,
                now_fn=clock,
            )
        )

    # The scan resumed at the start of the newest clip, not past it.
    first_query = FakeFrigateHandler.recording_requests[0][1]
    assert float(first_query.split("after=")[1].split("&")[0]) == float(BASE_TS) + 2

    # The archived chunk was not fetched or rewritten; only the settled tail
    # T+600..T+610 was archived now.
    assert FakeFrigateHandler.requested_clips == [float(BASE_TS + 600)]
    starts = [m.parse_s3_clip_start("cam_a", key) for key, _ in s3.puts]
    assert starts == [float(BASE_TS + 600)]


def test_unparseable_keys_are_no_better_than_an_empty_bucket():
    """Foreign or corrupt objects must not invent a watermark."""
    s3 = FakeS3Client(
        existing_keys=["cam_a/junk.mp4", "other_cam/2026/09/26/09/00-00.mp4"]
    )

    last = m.get_last_processed_time(
        camera="cam_a",
        s3_bucket="bucket",
        s3_client=s3,
        now_ts=BASE_TS,
        lookback_seconds=3600,
    )

    assert last is None


def test_the_first_run_window_is_never_narrower_than_the_watermark_lookback():
    """A newest clip outside the lookback window falls back to the first run.

    get_last_processed_time returns None when the newest clip is older than
    watermark_lookback_seconds, and the restart then starts
    first_run_lookback_seconds ago. If that fallback were narrower than the
    lookback it scanned, downtime would silently skip footage.
    """
    assert (
        m.DEFAULT_FIRST_RUN_LOOKBACK_SECONDS
        >= m.DEFAULT_WATERMARK_LOOKBACK_SECONDS
    )


def test_a_clip_older_than_the_lookback_window_resumes_from_the_wider_fallback():
    old_start = BASE_TS - 7200
    s3 = FakeS3Client(
        existing_keys=[m.make_s3_key("cam_a", float(old_start))]
    )

    outside = m.get_last_processed_time(
        camera="cam_a",
        s3_bucket="bucket",
        s3_client=s3,
        now_ts=BASE_TS,
        lookback_seconds=3600,
    )
    inside = m.get_last_processed_time(
        camera="cam_a",
        s3_bucket="bucket",
        s3_client=s3,
        now_ts=BASE_TS,
        lookback_seconds=3 * 3600,
    )

    assert outside is None
    assert inside == int(old_start)


def test_an_existing_key_is_accepted_as_archive_proof_for_a_longer_interval(
    frigate_server,
):
    """Accepted limitation, pinned so a change is always deliberate.

    A key proves a clip start, not its end. Here motion grew from
    T..T+610 to T..T+650, and the bucket already holds the key for start
    T+600, which covers only T+600..T+610. On restart the scan proposes
    T+600..T+650, the existing key is treated as sufficient, and the extra
    40 seconds are not fetched - the watermark advances to T+650 anyway.
    This is safe in practice because Frigate freezes a segment once a newer
    one exists and a closed interval cannot grow afterwards; it is the one
    loss scenario the bucket-only watermark accepts (see README.md
    "Crash recovery").
    """
    client = make_client(frigate_server)
    FakeFrigateHandler.segments_by_camera["cam_a"] = make_segments(
        [1] * 65 + [0] * 5
    )
    s3 = FakeS3Client(
        existing_keys=[m.make_s3_key("cam_a", float(BASE_TS + 600))]
    )

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                watermark_lookback_seconds=3600,
                first_run_lookback_seconds=612,
                idle_sleep_seconds=0.01,
                now_fn=SequencedClock([BASE_TS + 700, BASE_TS + 700]),
            )
        )

    assert FakeFrigateHandler.requested_clips == []
    assert s3.puts == []


CLIP_WINDOW = (float(BASE_TS), float(BASE_TS + SEGMENT_SECONDS))


def test_truncation_without_retries_raises(frigate_server):
    """With retries disabled a truncated clip is a failure, not a save.

    Only the exhausted retry loop may produce a -truncated key; with
    truncated clip_retries=0 the error must propagate like any other Frigate
    fault and the partial body must never be uploaded.
    """
    FakeFrigateHandler.truncate_clip_attempts = None
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(m.truncated_clip_error):
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=CLIP_WINDOW[0],
                end_ts=CLIP_WINDOW[1],
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                clip_retries=0,
            )
        )

    assert s3.puts == []
    assert s3.completed == []
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == 1


def test_multipart_truncated_save_keeps_every_part(frigate_server):
    """The multipart path gets the same treatment as the small-PUT path.

    A clip larger than part_size streams through parts and the trailer check
    runs before complete_multipart_upload; the final truncated save must go
    through the multipart path too, uploading the cut body in full.
    """
    FakeFrigateHandler.truncate_clip_attempts = None
    FakeFrigateHandler.clip_bytes = 12 * 1024 * 1024
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=1,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, True)
    assert s3.puts == []
    assert len(s3.completed) == 1
    key, _upload_id = s3.completed[0]
    assert key.endswith("-truncated.mp4")
    # the two failing attempts aborted their uploads; only the save pass completed
    assert len(s3.aborted) == 2
    saved = sorted((part, size) for k, part, size in s3.parts if k == key)
    assert [part for part, _ in saved] == list(range(1, len(saved) + 1))
    assert sum(size for _, size in saved) == (
        len(FakeFrigateHandler.clip_body()) - 8
    )


def test_all_truncated_retries_save_under_truncated_key(frigate_server):
    """Every attempt truncated -> the partial body is saved as -truncated.mp4.

    The retry loop re-requests the same window, and only once every attempt
    comes back truncated does a final fetch archive the body under the
    suffixed key, so the footage survives and the watermark can advance.
    """
    FakeFrigateHandler.truncate_clip_attempts = None
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=2,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, True)
    # 2 retries + the original + the final save fetch
    attempts = FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0])
    assert attempts == 4
    assert len(s3.puts) == 1
    key, size = s3.puts[0]
    assert key == m.make_s3_key(
        "cam_a", BASE_TS, suffix=m.TRUNCATED_KEY_SUFFIX
    )
    assert key.endswith("-truncated.mp4")
    # the partial body must be what Frigate actually served, not the full one
    assert size == len(FakeFrigateHandler.clip_body()) - 8


def test_a_later_complete_attempt_never_uses_the_truncated_key(
    frigate_server,
):
    """One complete retry is enough: no truncated key, no truncated flag."""
    FakeFrigateHandler.truncate_clip_attempts = 1
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=3,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, False)
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == 2
    assert len(s3.puts) == 1
    assert s3.puts[0][0] == m.make_s3_key("cam_a", BASE_TS)


def test_an_http_failure_never_produces_a_truncated_key(frigate_server):
    """A 5xx is the normal error path: raise it, never retry-as-truncated."""
    client = make_client(frigate_server)
    FakeFrigateHandler.clip_failures = {CLIP_WINDOW[0]}
    s3 = FakeS3Client()

    with pytest.raises(Exception) as excinfo:
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=CLIP_WINDOW[0],
                end_ts=CLIP_WINDOW[1],
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                clip_retries=3,
                clip_retry_max_delay=0,
            )
        )

    assert not isinstance(excinfo.value, m.truncated_clip_error)
    assert s3.puts == []
    # the truncated retry loop must not even look at Frigate again
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == 1


def test_a_proxy_503_after_a_truncated_attempt_saves_nothing(
    frigate_server,
):
    """Frigate dying behind a proxy: truncated, then 503 - no -truncated.mp4.

    The truncated save requires every attempt to have come back truncated.
    Once the proxy answers 503 for the down Frigate, the service fault ends
    the chain as a plain error: the window stays open for a later pass, and
    neither the partial body nor any marker is written.
    """
    FakeFrigateHandler.clip_sequences = {
        CLIP_WINDOW[0]: ["truncate", 503]
    }
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=CLIP_WINDOW[0],
                end_ts=CLIP_WINDOW[1],
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                clip_retries=3,
                clip_retry_max_delay=0,
            )
        )

    assert excinfo.value.response.status_code == 503
    assert s3.puts == []
    # the 503 escapes the loop: no further attempts, no final save fetch
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == 2


@pytest.mark.parametrize(
    "sequence, closing_name",
    [
        (["truncate", "norecordings"], "marker"),
        (["truncate", "empty"], "marker"),
        (["norecordings", "truncate"], "-truncated.mp4"),
        (["empty", "truncate"], "-truncated.mp4"),
    ],
)
def test_a_mixed_chain_closes_nothing(frigate_server, sequence, closing_name):
    """A chain whose fault changed type seals nothing.

    The truncated save and the no-recordings marker each claim the window is
    settled in a specific way, so they require every attempt of the chain to
    carry the same fault. A window that streamed a real body once and then
    answered unavailable (or the reverse) is evidence about neither fault
    alone: the chain raises, writes neither object, and the window is retried
    with a fresh chain on a later pass.
    """
    FakeFrigateHandler.clip_sequences = {CLIP_WINDOW[0]: list(sequence)}
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(Exception):
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=CLIP_WINDOW[0],
                end_ts=CLIP_WINDOW[1],
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                clip_retries=len(sequence) - 1,
                clip_retry_max_delay=0,
            )
        )

    # The last chain fault propagates - the task fails and the window stays
    # open - but neither closing object is written: no marker on a chain that
    # streamed a body, no -truncated.mp4 on a chain that said no-recordings.
    assert s3.puts == [], f"{closing_name} must not be written"
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == len(sequence)


def test_an_exhaustive_unavailable_chain_still_writes_the_marker(
    frigate_server,
):
    """The purity guard must not disarm the marker for a uniform chain."""
    FakeFrigateHandler.clip_sequences = {
        CLIP_WINDOW[0]: ["norecordings", "empty", "norecordings"]
    }
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=2,
            clip_retry_max_delay=0,
        )
    )

    # 400 and empty-200 are one family (recordings gone), so the chain is
    # uniform and the marker closes the window.
    assert (is_new, was_truncated) == (True, False)
    assert len(s3.puts) == 1
    assert s3.puts[0][0].endswith(m.UNAVAILABLE_KEY_SUFFIX)


def test_truncated_suffix_parses_back_to_the_clip_start():
    """The watermark scan must see truncated clips at their true start."""
    for start_ts in (BASE_TS, BASE_TS + 222):
        for suffix in ("", m.TRUNCATED_KEY_SUFFIX):
            key = m.make_s3_key("cam_a", start_ts, suffix=suffix)
            assert m.parse_s3_clip_start("cam_a", key) == int(start_ts)


def test_truncated_key_marks_the_window_archived(frigate_server):
    """A truncated clip is the last word Frigate gives for its window.

    Re-downloading it every pass would stream the same broken export
    forever, so an existing -truncated key satisfies the exists-check for
    both keys, and the watermark (which parses the suffix) moves past it.
    """
    client = make_client(frigate_server)
    truncated_key = m.make_s3_key(
        "cam_a", BASE_TS, suffix=m.TRUNCATED_KEY_SUFFIX
    )
    s3 = FakeS3Client(existing_keys=[truncated_key])

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )

    assert (is_new, was_truncated) == (False, False)
    assert FakeFrigateHandler.clip_attempts == []
    assert s3.puts == []


def test_run_camera_survives_a_permanently_truncated_camera(frigate_server):
    """A camera whose export always gets cut still advances the watermark.

    The truncated clip is archived under the suffixed key and the pass
    finishes; the next pass finds the archived clip in the bucket instead of
    retrying the same window forever, and the process keeps running.

    The clock answers two calls per pass (watermark resolve, pass end), so
    four values script exactly two passes: one that archives, one that
    proves recovery, then the loop is stopped.
    """
    FakeFrigateHandler.truncate_clip_attempts = None
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                idle_sleep_seconds=0.01,
                clip_retries=1,
                clip_retry_max_delay=0,
                now_fn=SequencedClock([BASE_TS + 200] * 4),
            )
        )

    assert s3.puts, "the truncated clip was never archived"
    assert all(
        key.endswith("-truncated.mp4") for key, _ in s3.puts
    ), s3.puts
    # pass 2 re-scans from the truncated clip's end and uploads nothing:
    # the bucket already covers everything the scan can settle on.
    attempts = [
        start
        for start in FakeFrigateHandler.clip_attempts
        if start > BASE_TS
    ]
    assert all(
        FakeFrigateHandler.clip_attempts.count(start) == 3
        for start in set(attempts)
    ), FakeFrigateHandler.clip_attempts


def test_no_recordings_window_closes_with_a_marker(frigate_server):
    """A window Frigate lists but cannot serve is closed, not retried forever.

    The scan returned segments for this window moments ago, so a clip endpoint
    answering 400 "No recordings found" means the files are gone from disk and
    no retry brings them back. Once every attempt repeats the 400, an empty
    marker object closes the window so the watermark can move past it.
    """
    FakeFrigateHandler.no_recordings_attempts = None
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=2,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, False)
    # 2 retries + the original attempt
    attempts = FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0])
    assert attempts == 3
    assert len(s3.puts) == 1
    key, size = s3.puts[0]
    assert key == m.make_unavailable_s3_key("cam_a", CLIP_WINDOW[0])
    assert key.endswith("-no-recordings")
    assert not key.endswith(".mp4"), "a marker holds no media"
    assert size == 0, "a marker is empty"


def test_a_later_successful_clip_needs_no_marker(frigate_server):
    """The marker path opens only on the no-recordings 400, once per window.

    A window that serves a clip on a later attempt is archived as a clip and
    no marker is written, so a marker can never shadow real footage.
    """
    FakeFrigateHandler.no_recordings_attempts = 1
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=2,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, False)
    assert len(s3.puts) == 1
    assert s3.puts[0][0] == m.make_s3_key("cam_a", CLIP_WINDOW[0])
    assert s3.puts[0][1] == len(FakeFrigateHandler.clip_body())


def test_an_unrelated_400_never_writes_a_marker(frigate_server):
    """Only Frigate's own message closes a window.

    A 400 from anything else - an HTML error page from a proxy, say - stays a
    Frigate fault: it must pin the watermark and be retried, because the
    recordings themselves are intact.
    """
    FakeFrigateHandler.reject_clips_400 = {CLIP_WINDOW[0]}
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(m.requests.exceptions.HTTPError) as excinfo:
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=CLIP_WINDOW[0],
                end_ts=CLIP_WINDOW[1],
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                clip_retries=1,
                clip_retry_max_delay=0,
            )
        )

    assert excinfo.value.response.status_code == 400
    assert not isinstance(excinfo.value, m.no_recordings_error)
    assert s3.puts == []
    # a service fault is not retried here: the supervisor owns that backoff
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == 1


def test_an_empty_clip_closes_the_window_with_a_marker(frigate_server):
    """HTTP 200 with no body is the same fault as the no-recordings 400.

    Frigate commits the 200 before ffmpeg runs, so an export that dies
    before its first packet - files the database still lists are unreadable
    - reaches the client as a successful empty response. Repeated on every
    attempt, no retry can export the window, so it is closed by a marker.
    """
    FakeFrigateHandler.empty_clip_attempts = None
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=2,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, False)
    # 2 retries + the original attempt
    attempts = FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0])
    assert attempts == 3
    assert len(s3.puts) == 1
    key, size = s3.puts[0]
    assert key == m.make_unavailable_s3_key("cam_a", CLIP_WINDOW[0])
    assert size == 0, "a marker is empty"


def test_a_retried_empty_clip_archives_the_clip_without_a_marker(
    frigate_server,
):
    """One non-empty retry wins: the window is a clip, not a marker.

    ffmpeg can die before its first packet for reasons that pass (an OOM
    victim, a transient I/O error), so the empty answer is retried, and a
    later real body is archived normally.
    """
    FakeFrigateHandler.empty_clip_attempts = 1
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
            clip_retries=2,
            clip_retry_max_delay=0,
        )
    )

    assert (is_new, was_truncated) == (True, False)
    assert len(s3.puts) == 1
    assert s3.puts[0][0] == m.make_s3_key("cam_a", CLIP_WINDOW[0])
    assert s3.puts[0][1] == len(FakeFrigateHandler.clip_body())


def test_a_truncated_then_empty_chain_seals_nothing(frigate_server):
    """A longer mixed chain also seals nothing.

    The first export comes back cut, every retry answers empty. Neither
    closing object may be written on such a chain: the marker would claim no
    recording was ever exportable although a real body streamed once, and
    the truncated save would claim a permanent export fault although the
    window now says the recordings are gone. The chain raises, writes
    nothing, and a later pass retries it with a fresh chain.
    """
    FakeFrigateHandler.clip_sequences = {
        CLIP_WINDOW[0]: ["truncate", "empty", "empty"]
    }
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(metrics.EmptyClipError):
        run(
            m.upload_clip_hybrid(
                camera="cam_a",
                start_ts=CLIP_WINDOW[0],
                end_ts=CLIP_WINDOW[1],
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                clip_retries=2,
                clip_retry_max_delay=0,
            )
        )

    assert s3.puts == []
    assert FakeFrigateHandler.clip_attempts.count(CLIP_WINDOW[0]) == 3


def test_existing_marker_closes_the_window_without_a_request(frigate_server):
    """A window already closed by a marker is never re-requested.

    The exists-check accepts the marker alongside the clip and truncated keys,
    so a pass after a restart does not re-fetch a window that can never be
    served again.
    """
    client = make_client(frigate_server)
    marker_key = m.make_unavailable_s3_key("cam_a", BASE_TS)
    s3 = FakeS3Client(existing_keys=[marker_key])

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=s3,
        )
    )

    assert (is_new, was_truncated) == (False, False)
    assert FakeFrigateHandler.clip_attempts == []
    assert s3.puts == []


def test_marker_suffix_parses_back_to_the_clip_start():
    """The watermark scan must see markers at their true window start."""
    for start_ts in (BASE_TS, BASE_TS + 222):
        key = m.make_unavailable_s3_key("cam_a", start_ts)
        assert m.parse_s3_clip_start("cam_a", key) == int(start_ts)


def test_a_marker_key_shape_that_is_not_ours_does_not_close_a_window():
    """Parsing stays exact, so a stray object cannot silently skip footage."""
    for key in (
        "cam_a/2026/09/26/09/46-40-no-recordings.mp4",
        "cam_a/2026/09/26/09/46-40-no-recordings.txt",
        "cam_a/2026/09/26/09/46-40-truncated",
        "cam_a/2026/09/26/09/46-40.txt",
    ):
        assert m.parse_s3_clip_start("cam_a", key) is None


def test_run_camera_survives_a_window_with_lost_recordings(frigate_server):
    """A camera whose recordings vanished from disk still finishes its passes.

    The marker closes each such window, so the next pass re-scans past them
    instead of retrying them forever, and the process keeps running.
    """
    FakeFrigateHandler.no_recordings_attempts = None
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                idle_sleep_seconds=0.01,
                clip_retries=1,
                clip_retry_max_delay=0,
                now_fn=SequencedClock([BASE_TS + 200] * 4),
            )
        )

    assert s3.puts, "no window was closed"
    assert all(key.endswith("-no-recordings") for key, _ in s3.puts), s3.puts
    assert all(size == 0 for _, size in s3.puts), s3.puts
    # two windows, one attempt and one retry each; pass 2 asks for no clip at
    # all, because the markers already cover everything the scan settles on.
    assert len(FakeFrigateHandler.clip_attempts) == 4, (
        FakeFrigateHandler.clip_attempts
    )


def test_a_configured_max_gap_splits_intervals_it_cannot_bridge(
    frigate_server,
):
    """cam_a has motion at segments 0-2 and 4, a 10 s gap at segment 3.

    The default gap bridges it into one interval; a smaller configured gap
    must split it, so a camera that loses segments gets separate clips
    instead of one clip with a hole inside.
    """
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                idle_sleep_seconds=0.01,
                max_gap_seconds=5,
                now_fn=SequencedClock([BASE_TS + 200] * 4),
            )
        )

    assert FakeFrigateHandler.requested_clips == [
        float(BASE_TS),
        float(BASE_TS + 40),
    ]


def test_run_camera_writes_under_the_configured_prefix(frigate_server):
    client = make_client(frigate_server)
    s3 = FakeS3Client()

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                idle_sleep_seconds=0.01,
                key_prefix="frigate/archive/",
                now_fn=SequencedClock([BASE_TS + 200] * 4),
            )
        )

    assert s3.puts, "no clip was archived"
    assert all(
        key.startswith("frigate/archive/cam_a/") for key, _ in s3.puts
    ), s3.puts


def test_watermark_lookup_only_sees_the_configured_prefix():
    """With a prefix in use, a prefixless listing finds nothing, which is the
    failure mode of changing S3_PREFIX on a populated bucket: it must show up
    as an empty lookup, not as a wrong watermark."""
    key = m.make_s3_key("cam_a", float(BASE_TS - 600), key_prefix="frigate/")
    s3 = FakeS3Client(existing_keys=[key])

    found = m.get_last_processed_time(
        camera="cam_a",
        s3_bucket="bucket",
        s3_client=s3,
        now_ts=BASE_TS,
        lookback_seconds=3600,
        key_prefix="frigate/",
    )
    missed = m.get_last_processed_time(
        camera="cam_a",
        s3_bucket="bucket",
        s3_client=s3,
        now_ts=BASE_TS,
        lookback_seconds=3600,
    )

    assert found == int(BASE_TS - 600)
    assert missed is None


def _signal_main_env(frigate_server, monkeypatch):
    """Patch main()'s world: one camera, fake S3, fake Frigate, port 0."""
    async def fake_fetch(frigate_client, timeout=None):
        return ["cam_a"]

    async def fake_run_camera(camera, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setenv("FRIGATE_URL", frigate_server)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("FRIGATE_USER", "")
    monkeypatch.setenv("FRIGATE_PASSWORD", "")
    monkeypatch.setenv("METRICS_PORT", "0")
    monkeypatch.setattr(m.boto3, "client", lambda name, **kw: FakeS3Client())
    monkeypatch.setattr(m, "fetch_cameras", fake_fetch)
    monkeypatch.setattr(m, "run_camera", fake_run_camera)
    return fake_fetch


def test_sigint_exits_cleanly_through_the_main_path(frigate_server, monkeypatch):
    """Ctrl-C stops the run through cancellation: exit code 0, no exit error.

    Without signal handlers SIGINT lands as a KeyboardInterrupt on some
    interpreter line and asyncio.run re-raises it with a traceback; main()
    must route the signal through the cooperative cancellation path instead.
    main() owns asyncio.run, so the signal must arrive from a thread - the
    loop's signal handler is what wakes the main thread.
    """
    _signal_main_env(frigate_server, monkeypatch)

    started = threading.Event()
    real_run_cameras = m._run_cameras

    async def spy_run_cameras(cfg, s3_bucket, key_timezone, now_fn):
        started.set()
        return await real_run_cameras(cfg, s3_bucket, key_timezone, now_fn)

    monkeypatch.setattr(m, "_run_cameras", spy_run_cameras)
    sender = threading.Thread(
        target=lambda: (started.wait(10), os.kill(os.getpid(), signal.SIGINT)),
        daemon=True,
    )
    sender.start()

    with pytest.raises(SystemExit) as exit_info:
        m.main()
    sender.join(timeout=10)

    assert exit_info.value.code == 0


def test_signal_during_camera_discovery_exits_cleanly(frigate_server, monkeypatch):
    """A signal while discovery is retrying stops discovery, not just cameras.

    Discovery runs before any supervisor exists. Its retry loop awaits a
    sleep between attempts; the stop must cancel it cooperatively and still
    exit 0 rather than unwind through the retry loop.
    """
    async def failing_fetch(frigate_client, timeout=None):
        raise ConnectionError("frigate is down")

    monkeypatch.setenv("FRIGATE_URL", frigate_server)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("FRIGATE_USER", "")
    monkeypatch.setenv("FRIGATE_PASSWORD", "")
    monkeypatch.setenv("METRICS_PORT", "0")
    monkeypatch.setattr(m.boto3, "client", lambda name, **kw: FakeS3Client())
    monkeypatch.setattr(m, "fetch_cameras", failing_fetch)

    sender = threading.Thread(
        target=lambda: (time.sleep(0.3), os.kill(os.getpid(), signal.SIGINT)),
        daemon=True,
    )
    sender.start()
    with pytest.raises(SystemExit) as exit_info:
        m.main()
    sender.join(timeout=10)

    assert exit_info.value.code == 0


def test_metrics_port_is_free_after_a_signal_stop(frigate_server, monkeypatch):
    """The /metrics listener must not outlive the run."""
    _signal_main_env(frigate_server, monkeypatch)

    bound = {}
    real_start = m.metrics.start_metrics_server

    def spy_start(port=9108, bind=None):
        bound["port"] = real_start(port=0)
        return bound["port"]

    monkeypatch.setattr(m.metrics, "start_metrics_server", spy_start)

    def send_int():
        for _ in range(2000):
            if bound:
                break
            time.sleep(0.005)
        os.kill(os.getpid(), signal.SIGINT)

    sender = threading.Thread(target=send_int, daemon=True)
    sender.start()
    with pytest.raises(SystemExit) as exit_info:
        m.main()
    sender.join(timeout=10)

    assert exit_info.value.code == 0
    # The listener must be gone: a connect to the old port is refused.
    with pytest.raises(OSError):
        probe = socket.create_connection(("127.0.0.1", bound["port"]), timeout=2)
        probe.close()


def test_second_signal_kills_a_wedged_shutdown():
    """A shutdown that ignores cancellation must remain killable.

    The first signal removes both handlers before cancelling, so a second
    one lands as the default KeyboardInterrupt instead of being swallowed.
    """
    wedged = threading.Event()

    async def body():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            wedged.set()
            await asyncio.sleep(30)  # a shutdown that never completes

    async def scenario():
        return await m.run_until_signalled(body())

    def send_signals():
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGINT)
        for _ in range(2000):
            if wedged.is_set():
                break
            time.sleep(0.005)
        os.kill(os.getpid(), signal.SIGINT)

    sender = threading.Thread(target=send_signals, daemon=True)
    sender.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(scenario())
    finally:
        sender.join(timeout=10)



def test_a_camera_pass_is_followable_at_info_without_clip_noise(
    frigate_server, caplog
):
    """One archived pass at INFO: what the camera did, but not every key.

    The whole point of levels: docker logs shows intervals found and clips
    uploaded per pass, while per-clip S3 keys - the spam - stay at DEBUG.
    """
    client = make_client(frigate_server)
    s3 = FakeS3Client()
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    def text_at(level):
        return " | ".join(
            record.getMessage()
            for record in caplog.records
            if record.levelno == level
            and record.name == "frigate_s3_archiver"
        )

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    info_text = text_at(logging.INFO)
    assert "Intervals found: 2" in info_text
    assert "Uploaded: 2, already present: 0" in info_text
    assert "No clips found in S3. Starting at 20" in info_text

    debug_text = text_at(logging.DEBUG)
    assert "Uploaded (single PUT): cam_a/" in debug_text
    assert "No intervals. Sleeping" in debug_text

    # A healthy pass must not warn: no truncation, no markers, no restarts.
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_start_camera_binds_the_camera_for_its_tasks_lines(
    frigate_server, monkeypatch
):
    """Every line a camera task logs carries its camera, through the JSON
    formatter's field - not a prefix parsed back out of the message."""
    _signal_main_env(frigate_server, monkeypatch)

    async def two_cameras(frigate_client, timeout=None):
        return ["cam_a", "cam_b"]

    async def logging_camera(camera, **kwargs):
        logging.getLogger("frigate_s3_archiver").info(
            "pass done by %s", camera
        )
        await asyncio.Event().wait()

    monkeypatch.setattr(m, "run_camera", logging_camera)
    monkeypatch.setattr(m, "fetch_cameras", two_cameras)

    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    stream = io.StringIO()
    logging_setup.configure_logging(
        level=logging.DEBUG, json_format=True, stream=stream
    )
    try:
        cfg = load_config()

        async def scenario():
            run_task = asyncio.create_task(
                m.async_main(config=cfg, now_fn=lambda: END_TS)
            )
            for _ in range(200):
                if "pass done by cam_b" in stream.getvalue():
                    break
                await asyncio.sleep(0.01)
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

        run(scenario())
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        logging_setup._camera_var.set("")

    payloads = [
        json.loads(line)
        for line in stream.getvalue().splitlines()
        if json.loads(line).get("logger") == "frigate_s3_archiver"
    ]
    done = [p for p in payloads if p["message"].startswith("pass done by")]
    assert sorted(p["message"] for p in done) == [
        "pass done by cam_a",
        "pass done by cam_b",
    ]
    # The field, not the message: each camera tagged its own lines.
    assert all(p["message"].endswith(p["camera"]) for p in done)
    # The startup enumeration names cameras in its message but belongs to no
    # camera, so it must carry no camera field.
    listed = [p for p in payloads if p["message"].startswith("Cameras:")]
    assert listed and all("camera" not in p for p in listed)


def test_human_ts_renders_the_instant_in_the_key_timezone():
    assert m.human_ts(1700000000, ZoneInfo("UTC")) == (
        "2023-11-14 22:13:20 UTC"
    )
    assert m.human_ts(1700000000, ZoneInfo("Europe/Moscow")) == (
        "2023-11-15 01:13:20 MSK"
    )


def test_sdk_retries_log_as_debug_lines(monkeypatch, caplog):
    """A retry inside the SDK, invisible to our code, becomes our own line.

    boto3 retries an S3 operation up to its attempt budget without our code
    or a task restart noticing - the gap the metric never shows. Against a
    dead endpoint with a real client, every failed attempt logs one of our
    DEBUG lines naming the operation and the reason; the SDK's own retry
    logger is silenced, so DEBUG stays readable.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:9")
    monkeypatch.setenv("LOG_JSON", "false")
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    client = m.make_s3_client(
        connect_timeout_seconds=1, read_timeout_seconds=1, total_max_attempts=2
    )
    with pytest.raises(EndpointConnectionError):
        client.head_object(Bucket="bucket", Key="cam_a/k.mp4")

    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    attempts = [line for line in text if "S3 attempt" in line]
    assert len(attempts) == 2, attempts
    assert all("HeadObject" in line for line in attempts)
    assert "S3 attempt 1 failed during HeadObject" in attempts[0]
    assert "S3 attempt 2 failed during HeadObject" in attempts[1]
    assert all("EndpointConnectionError" in line for line in attempts)


def test_a_truncated_retry_pause_logs_the_fault_it_retries(
    frigate_server, caplog
):
    """The silent sleep between clip attempts says what it waits out."""
    FakeFrigateHandler.truncate_clip_attempts = 1
    client = make_client(frigate_server)
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    is_new, was_truncated = run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=FakeS3Client(),
            clip_retries=1,
            clip_retry_max_delay=1,
        )
    )

    assert (is_new, was_truncated) == (True, False)
    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    pause = [line for line in text if "came back truncated" in line]
    assert len(pause) == 1
    assert "Attempt 1/2" in pause[0]
    assert "retrying in 1.0 s" in pause[0]


def test_an_unavailable_retry_pause_logs_the_fault_it_retries(
    frigate_server, caplog
):
    """The unavailable-window pause says which answer it waits out."""
    FakeFrigateHandler.no_recordings_attempts = None
    client = make_client(frigate_server)
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    run(
        m.upload_clip_hybrid(
            camera="cam_a",
            start_ts=CLIP_WINDOW[0],
            end_ts=CLIP_WINDOW[1],
            frigate_client=client,
            s3_bucket="bucket",
            s3_client=FakeS3Client(),
            clip_retries=1,
            clip_retry_max_delay=1,
        )
    )

    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    pause = [line for line in text if "found the recordings unavailable" in line]
    assert len(pause) == 1
    assert "NoRecordingsError" in pause[0]
    assert "retrying in 1.0 s" in pause[0]


def test_the_scan_logs_each_queried_window(frigate_server, caplog):
    """Each Frigate recordings query is visible at DEBUG."""
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    scan_window(frigate_server, [1, 1, 0, 0], scan_segments=4)

    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    queried = [line for line in text if line.startswith("Segments queried")]
    assert len(queried) == 1
    assert "4 returned" in queried[0]
    assert str(BASE_TS) in queried[0]


def test_the_watermark_listing_logs_each_prefix_it_walks(caplog):
    """A watermark read stuck on a slow endpoint shows its last prefix."""
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    m.get_last_processed_time(
        camera="cam_a",
        s3_bucket="bucket",
        s3_client=FakeS3Client(),
        now_ts=BASE_TS,
        lookback_seconds=3600,
    )

    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    listings = [line for line in text if line.startswith("Listing cam_a/")]
    assert listings, text
    assert all(line.endswith("to find the newest clip") for line in listings)


def test_a_deferred_tail_logs_why_the_interval_waits(caplog):
    """The reason a fresh interval is not archived this pass is visible."""
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")

    chunks = m.split_interval_for_archive(
        interval_start=0,
        interval_end=700,
        scan_end_ts=750,
        chunk_seconds=600,
        max_gap_seconds=120,
        is_last_interval=True,
    )

    assert chunks == [(0, 600)]
    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    deferred = [line for line in text if "Deferred the ongoing tail" in line]
    assert len(deferred) == 1
    assert "600..700" in deferred[0]


def test_a_cancelled_worker_thread_logs_its_checkpoint(caplog):
    """Stopping for cancellation and a failed abort both leave a line.

    The abort swallowing is deliberate - the upload must fail with its own
    error - but the orphan it leaves in the bucket is billed, so the failure
    at least says so.
    """
    caplog.set_level(logging.DEBUG, logger="frigate_s3_archiver")
    cancel_event = threading.Event()

    class AbortingS3(FakeS3Client):
        def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
            cancel_event.set()
            return super().upload_part(
                Bucket, Key, UploadId, PartNumber, Body
            )

        def abort_multipart_upload(self, Bucket, Key, UploadId):
            raise RuntimeError("endpoint gone")

    s3 = AbortingS3()
    stream = iter([b"y" * (4 * 1024 * 1024)] * 4)

    class StreamingResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=None):
            return stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class StreamingSession:
        def get(self, url, stream=False, timeout=None):
            return StreamingResponse()

    with pytest.raises(asyncio.CancelledError):
        m._upload_clip_hybrid_sync(
            camera="cam_a",
            start_ts=float(BASE_TS),
            end_ts=float(BASE_TS + 10 * SEGMENT_SECONDS),
            frigate_client={
                "session": StreamingSession(),
                "base_url": "http://fake",
            },
            s3_bucket="bucket",
            s3_client=s3,
            key_timezone=m.DEFAULT_S3_KEY_TIMEZONE,
            part_size=m.DEFAULT_PART_SIZE,
            http_chunk_size=1024 * 1024,
            http_timeout=30,
            cancel_event=cancel_event,
        )

    assert s3.aborted == []
    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    assert any("Stopping the worker thread" in line for line in text)
    abort_line = [
        line for line in text if "Could not abort multipart upload" in line
    ]
    assert len(abort_line) == 1
    assert "upload-1" in abort_line[0]
    assert "endpoint gone" in abort_line[0]


def test_a_watermark_start_reads_as_a_date_not_a_bare_epoch(
    frigate_server, caplog
):
    client = make_client(frigate_server)
    s3 = FakeS3Client()
    caplog.set_level(logging.INFO, logger="frigate_s3_archiver")

    with pytest.raises(StopLoop):
        run(
            m.run_camera(
                camera="cam_a",
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                key_timezone=ZoneInfo("UTC"),
                now_fn=ScriptedClock(END_TS, calls=3),
                **run_camera_options(),
            )
        )

    info_text = " | ".join(
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    )
    assert "Starting at 20" in info_text
    assert "UTC" in info_text
    assert str(int(END_TS)) not in info_text


def test_main_exits_one_when_the_camera_selection_is_empty(
    frigate_server, monkeypatch, caplog
):
    """A filter that selects nothing means nothing is archived: error, 1."""
    _signal_main_env(frigate_server, monkeypatch)
    monkeypatch.setenv("CAMERAS_INCLUDE", "no-such-camera")
    monkeypatch.setattr(logging_setup, "configure_logging", lambda **kw: None)
    caplog.set_level(logging.INFO, logger="frigate_s3_archiver")

    with pytest.raises(SystemExit) as exit_info:
        m.main()

    assert exit_info.value.code == 1
    errors = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR
        and record.name == "frigate_s3_archiver"
    ]
    assert len(errors) == 1
    assert "CAMERAS_INCLUDE" in errors[0].getMessage()


def test_main_logs_the_fatal_supervisor_path_as_one_error_record(
    frigate_server, monkeypatch, caplog
):
    """A supervisor bug leaves one ERROR record carrying the traceback."""
    _signal_main_env(frigate_server, monkeypatch)

    async def broken_supervisor(**kwargs):
        raise RuntimeError("supervisor bug")

    monkeypatch.setattr(m, "supervise_camera", broken_supervisor)
    monkeypatch.setattr(logging_setup, "configure_logging", lambda **kw: None)
    caplog.set_level(logging.INFO, logger="frigate_s3_archiver")

    with pytest.raises(SystemExit) as exit_info:
        m.main()

    assert exit_info.value.code == 1
    errors = [
        record
        for record in caplog.records
        if record.levelno == logging.ERROR
        and record.name == "frigate_s3_archiver"
    ]
    assert [r.getMessage() for r in errors] == [
        "Archiver stopped by an unhandled error."
    ]
    assert errors[0].exc_info is not None


def test_main_exits_one_when_startup_discovery_gives_up(
    frigate_server, monkeypatch, caplog
):
    """The retry warnings, the giving-up error, exit 1 - in that order."""
    monkeypatch.setenv("FRIGATE_URL", frigate_server)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("FRIGATE_USER", "")
    monkeypatch.setenv("FRIGATE_PASSWORD", "")
    monkeypatch.setattr(m.boto3, "client", lambda name, **kw: FakeS3Client())
    monkeypatch.setattr(
        m.metrics, "start_metrics_server", lambda port, bind=None: port
    )

    async def always_down(frigate_client, timeout=None):
        raise ConnectionError("frigate is down")

    real_discovery = m.fetch_cameras_with_retry

    async def fast_discovery(frigate_client):
        return await real_discovery(
            frigate_client, attempts=2, retry_seconds=0.01
        )

    monkeypatch.setattr(m, "fetch_cameras", always_down)
    monkeypatch.setattr(m, "fetch_cameras_with_retry", fast_discovery)
    monkeypatch.setattr(logging_setup, "configure_logging", lambda **kw: None)
    caplog.set_level(logging.INFO, logger="frigate_s3_archiver")

    with pytest.raises(SystemExit) as exit_info:
        m.main()

    assert exit_info.value.code == 1
    text = [
        record.getMessage()
        for record in caplog.records
        if record.name == "frigate_s3_archiver"
    ]
    assert "Camera discovery failed (1/2): ConnectionError: frigate is down" in text
    assert "Camera discovery failed 2 times; giving up" in text[-2]
    assert text[-1] == "Archiver stopped by an unhandled error."
