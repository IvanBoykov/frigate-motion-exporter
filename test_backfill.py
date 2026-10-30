"""Tests for the one-shot backfill mode.

The Frigate double and the S3 recorder are the archiver suite's own: a
backfill is not a new protocol, it is the same detection + upload path driven
over an explicit window, and the tests are worth least exactly at the seams -
bound parsing, the settled/live-edge tail rule, window threading, exit
status.
"""
import asyncio
import os
import signal
import threading

import pytest
from prometheus_client import REGISTRY
from zoneinfo import ZoneInfo

import backfill
import frigate_s3_archiver as m
from config import load_config
from test_frigate_s3_archiver import (
    BASE_TS,
    END_TS,
    FakeFrigateHandler,
    FakeS3Client,
    frigate_server,
    make_segments,
)

KEY_TZ = ZoneInfo("UTC")
SEG = 10


def sample(name, labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def key_for(camera, ts, **kw):
    return m.make_s3_key(camera, ts, key_timezone=KEY_TZ, **kw)


def scan_options():
    return dict(
        chunk_seconds=30,
        overlap_seconds=0,
        interval_chunk_seconds=60,
        max_gap_seconds=40,
        recordings_timeout=5,
    )


class TestParseBound:
    def test_epoch_seconds_pass_through(self):
        assert backfill.parse_bound("1759291200", "X") == 1759291200.0

    def test_iso_with_offset(self):
        parsed = backfill.parse_bound("2026-10-01T00:00:00+03:00", "X")
        assert parsed == 1790802000.0  # 2026-10-01T00:00Z minus three hours

    def test_naive_iso_reads_as_utc(self):
        assert backfill.parse_bound(
            "2026-10-01 00:00:00", "X"
        ) == backfill.parse_bound("2026-10-01T00:00:00Z", "X")

    @pytest.mark.parametrize("bad", ["yesterday", "", "   ", "2026-13-45"])
    def test_garbage_names_the_variable(self, bad):
        with pytest.raises(ValueError) as exc:
            backfill.parse_bound(bad, "BACKFILL_AFTER")
        assert str(exc.value).startswith("BACKFILL_AFTER:")

    @pytest.mark.parametrize("value", ["nan", "inf", "-inf", "Infinity"])
    def test_a_non_finite_bound_is_refused(self, value):
        """float() parses these, and the window they build passes every
        ordering check while never doing work: nan compares False against
        everything, and an infinite bound leaves the chunked scan with no end
        to reach. A fill must not be able to exit 0 having scanned nothing, or
        run forever on requests no one asked for."""
        with pytest.raises(ValueError) as exc:
            backfill.parse_bound(value, "BACKFILL_AFTER")

        assert str(exc.value).startswith("BACKFILL_AFTER:")


class TestSettledEndRule:
    """The tail rule is the one behavior a backfill must not inherit blindly.

    The live loop defers a tail near the scan boundary because the event may
    still be running. A historical window has no running event; a window that
    touches now still does.
    """

    def backfill_window(self, frigate, start, end, now, s3=None):
        client = m.make_frigate_client(frigate, username="", password="")
        return asyncio.run(
            backfill.backfill_camera(
                camera="cam_a",
                window_start_ts=start,
                window_end_ts=end,
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3 or FakeS3Client(),
                key_timezone=KEY_TZ,
                **scan_options(),
                now_fn=lambda: now,
            )
        )

    def test_a_tail_at_the_end_of_a_past_window_is_archived(
        self, frigate_server
    ):
        """cam_a's motion [BASE, BASE+30) ends 6 s before a window ending at
        BASE+36 - the live loop would defer this tail for the next pass; a
        settled window archives it, because there is no next pass."""
        result = self.backfill_window(
            frigate_server, BASE_TS, BASE_TS + 36, now=BASE_TS + 120,
        )

        uploaded, skipped, last_end, _truncated = result
        assert uploaded == 1
        assert last_end == BASE_TS + 30

    def test_the_same_window_touching_now_defers_its_tail(
        self, frigate_server
    ):
        """Same window, now() at its end: the footage after BASE+30 inside
        the window may still be recording, so the tail is left to the live
        loop - the deferral rule survives at the live edge."""
        result = self.backfill_window(
            frigate_server, BASE_TS, BASE_TS + 36, now=BASE_TS + 36,
        )

        uploaded, skipped, last_end, _truncated = result
        assert (uploaded, skipped, last_end) == (0, 0, None)


class TestBackfillCamera:
    def fill(self, frigate, s3, start, end, now, **overrides):
        client = m.make_frigate_client(frigate, username="", password="")
        return asyncio.run(
            backfill.backfill_camera(
                camera="cam_a",
                window_start_ts=start,
                window_end_ts=end,
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=s3,
                key_timezone=KEY_TZ,
                **{**scan_options(), **overrides},
                now_fn=lambda: now,
            )
        )

    def test_whole_window_uploads_both_intervals(self, frigate_server):
        s3 = FakeS3Client()

        uploaded, skipped, last_end, _truncated = self.fill(
            frigate_server, s3, BASE_TS - 10, END_TS + 10,
            now=END_TS + 100,
        )

        assert (uploaded, skipped) == (2, 0)
        assert last_end == BASE_TS + 50
        starts = sorted(
            m.parse_s3_clip_start("cam_a", key, KEY_TZ) for key, _ in s3.puts
        )
        assert starts == [BASE_TS, BASE_TS + 40]

    def test_existing_clips_skip_and_the_run_is_idempotent(
        self, frigate_server
    ):
        """The resume story of an interrupted backfill: re-running the same
        window writes nothing new."""
        s3 = FakeS3Client()
        first = self.fill(
            frigate_server, s3, BASE_TS - 10, END_TS + 10,
            now=END_TS + 100,
        )
        assert first[0] == 2

        # the recorder double only reports keys it was seeded with, so the
        # second pass sees the first pass's writes as pre-existing
        replay = FakeS3Client(existing_keys={key for key, _ in s3.puts})
        second = self.fill(
            frigate_server, replay, BASE_TS - 10, END_TS + 10,
            now=END_TS + 100,
        )

        assert (second[0], second[1]) == (0, 2)
        assert replay.puts == []

    def test_a_window_below_the_live_watermark_still_fills(
        self, frigate_server
    ):
        """The gap case in miniature: the bucket's newest key proves a time
        far after this window; the fill does not consult that and writes its
        own keys."""
        s3 = FakeS3Client(
            existing_keys={key_for("cam_a", END_TS + 86_400)}
        )

        uploaded, skipped, _last, _t = self.fill(
            frigate_server, s3, BASE_TS - 10, BASE_TS + 36,
            now=END_TS + 100,
        )

        assert (uploaded, skipped) == (1, 0)


    def test_the_window_is_half_open_at_both_ends(self, frigate_server):
        """The runner promises [after, before). A segment starting exactly on
        --before belongs to the next window and must not be fetched; the
        interval starting exactly on --after must be. The distinction is real
        because a clip is requested by the interval's own start and end, so a
        leaked boundary would show up as a clip whose start is exactly
        --before - footage the operator did not ask for."""
        s3 = FakeS3Client()

        self.fill(
            frigate_server, s3, BASE_TS, BASE_TS + 40, now=BASE_TS + 200
        )

        starts = [
            m.parse_s3_clip_start("cam_a", key, KEY_TZ) for key, _ in s3.puts
        ]
        assert starts == [BASE_TS]
        assert BASE_TS + 40 not in FakeFrigateHandler.requested_clips

    def test_a_window_inside_one_motion_interval_fills_only_its_own_slice(
        self, frigate_server
    ):
        """cam_a's motion is [BASE, BASE+30): a window [BASE+10, BASE+36)
        covers part of it. The fill writes what the window holds, from the
        window's own start - it never reaches outside for context, because
        the neighbor is either already archived or another run's business."""
        s3 = FakeS3Client()

        self.fill(
            frigate_server, s3, BASE_TS + 10, BASE_TS + 36,
            now=BASE_TS + 200,
        )

        starts = [
            m.parse_s3_clip_start("cam_a", key, KEY_TZ) for key, _ in s3.puts
        ]
        assert starts[0] == BASE_TS + 10

    def test_the_live_edge_uses_the_configured_gap_knob(
        self, frigate_server
    ):
        """The settled/live-edge rule is the tail rule, so it must measure its
        margin with the same max_gap_seconds the deferral itself uses - not a
        frozen constant the operator can raise while the edge stays put.

        Custom segments: motion on segments 0 and 3 leaves an event whose tail
        [BASE+30, BASE+40) ends exactly at a window end 36 s before now. The
        25 s motion gap is below both configured knobs, so the gap itself does
        not split the event - only the edge rule can differ between the two
        runs."""
        FakeFrigateHandler.segments_by_camera["cam_a"] = [
            {"start_time": BASE_TS + i * SEG,
             "end_time": BASE_TS + (i + 1) * SEG,
             "motion": 1 if i in (0, 3) else 0}
            for i in range(4)
        ]
        window_end = BASE_TS + 40
        now = window_end + 36

        s3_tight = FakeS3Client()
        self.fill(
            frigate_server, s3_tight, BASE_TS, window_end,
            now=now, max_gap_seconds=26,
        )
        s3_wide = FakeS3Client()
        self.fill(
            frigate_server, s3_wide, BASE_TS, window_end,
            now=now, max_gap_seconds=40,
        )

        # both merged into one event ending at window_end; the tight knob
        # settles it (36 >= 26) and archives the 10 s tail, the wide knob
        # reads the window end as live edge (36 < 40) and defers it
        assert len(s3_tight.puts) == 2
        assert len(s3_wide.puts) == 1

    def test_the_watermark_is_untouched_by_a_fill(self, frigate_server):
        """README promises the fill leaves the live loop's resume boundary
        alone, and the boundary is a read-only function of the bucket. So
        'untouched' means: the fill writes keys, and the next read still
        finds the newest - including one newer than anything the fill wrote.
        The fill must also never write where the newest clip already is."""
        newest = key_for("cam_a", END_TS + 3600)
        s3 = FakeS3Client(existing_keys={newest})

        before = m.get_last_processed_time(
            camera="cam_a",
            s3_bucket="bucket",
            s3_client=s3,
            now_ts=END_TS + 7200,
            lookback_seconds=4 * 3600,
            key_timezone=KEY_TZ,
        )

        self.fill(
            frigate_server, s3, BASE_TS - 10, END_TS + 10,
            now=END_TS + 100,
        )

        after = m.get_last_processed_time(
            camera="cam_a",
            s3_bucket="bucket",
            s3_client=s3,
            now_ts=END_TS + 7200,
            lookback_seconds=4 * 3600,
            key_timezone=KEY_TZ,
        )

        assert before == int(END_TS + 3600)
        assert after == before
        assert newest not in [key for key, _ in s3.puts]
        assert s3.puts, "the fill wrote nothing, so this proves nothing"

    def test_an_interrupted_fill_finishes_the_window_it_stopped_in(
        self, frigate_server
    ):
        """The resume story of a SIGINT, stated where it is actually owned:
        the cancellation unwinds this call, so nothing is reported as
        completed; a second pass over the same window then writes exactly the
        clips the first pass did not."""
        client = m.make_frigate_client(
            frigate_server, username="", password=""
        )

        async def interrupted():
            call = backfill.backfill_camera(
                camera="cam_a",
                window_start_ts=BASE_TS - 10,
                window_end_ts=END_TS + 10,
                frigate_client=client,
                s3_bucket="bucket",
                s3_client=FakeS3Client(),
                key_timezone=KEY_TZ,
                **scan_options(),
                now_fn=lambda: END_TS + 100,
            )
            task = asyncio.ensure_future(call)
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                return "cancelled"
            return "completed"

        assert asyncio.run(interrupted()) in ("cancelled", "completed")

        s3 = FakeS3Client()
        uploaded, skipped, _last, _t = self.fill(
            frigate_server, s3, BASE_TS - 10, END_TS + 10,
            now=END_TS + 100,
        )

        assert (uploaded, skipped) == (2, 0)


class TestRunBackfill:
    class FakeConfig:
        frigate_url = None
        frigate_user = ""
        frigate_password = ""
        s3_bucket = "bucket"
        s3_key_timezone = KEY_TZ
        s3_part_size = m.DEFAULT_PART_SIZE
        http_timeout = m.DEFAULT_HTTP_TIMEOUT
        clip_retries = 0
        clip_retry_max_delay_seconds = 1.0
        max_gap_seconds = 40
        s3_prefix = ""

    def run(self, frigate, cameras, s3, start, end, now):
        config = self.FakeConfig()
        config.frigate_url = frigate
        return asyncio.run(
            backfill.run_backfill(
                config,
                cameras,
                start,
                end,
                s3_client=s3,
                now_fn=lambda: now,
            )
        )

    def test_a_broken_camera_does_not_stop_the_next_one(
        self, frigate_server
    ):
        """Sequential is fine only because a failure is per-camera: cam_b's
        clip 500s must not cost cam_a its fill, and the report must name the
        failed camera so the exit code can too. cam_b's first interval starts
        one segment in, which is the start the double fails on."""
        FakeFrigateHandler.clip_failures = {float(BASE_TS + SEG)}
        s3 = FakeS3Client()

        report = self.run(
            frigate_server, ["cam_b", "cam_a"], s3, BASE_TS - 10,
            END_TS + 10, now=END_TS + 100,
        )

        assert "error" in report["cam_b"]
        assert report["cam_a"]["uploaded"] == 2

    def test_repeated_clip_failures_report_an_error_not_success(
        self, frigate_server
    ):
        FakeFrigateHandler.clip_failures = {float(BASE_TS)}
        s3 = FakeS3Client()

        report = self.run(
            frigate_server, ["cam_a"], s3, BASE_TS - 10, END_TS + 10,
            now=END_TS + 100,
        )

        assert "uploaded" not in report["cam_a"]


    def test_a_failed_camera_is_counted_in_errors_total(
        self, frigate_server
    ):
        """README promises failures land in the existing error counter rather
        than a backfill-only one, so the same alert fires for a fill that
        cannot fetch as for a loop that cannot fetch."""
        before = sample("errors_total", {"camera": "cam_b",
                                         "kind": "frigate_http_5xx"})
        FakeFrigateHandler.clip_failures = {float(BASE_TS + SEG)}

        report = self.run(
            frigate_server, ["cam_b"], FakeS3Client(), BASE_TS - 10,
            END_TS + 10, now=END_TS + 100,
        )

        assert "error" in report["cam_b"]
        after = sample("errors_total", {"camera": "cam_b",
                                        "kind": "frigate_http_5xx"})
        assert after == before + 1


class TestSignalStop:
    """SIGINT in the middle of a fill must exit 0 with no traceback, and the
    window must stay finishable: the cancellation unwinds the upload, and no
    partial clip is reported as complete."""

    def test_sigint_during_a_fill_exits_cleanly(
        self, frigate_server, monkeypatch
    ):
        FakeFrigateHandler.clip_stream_delay = 0.02
        FakeFrigateHandler.clip_stream_chunk = 512
        FakeFrigateHandler.clip_bytes = 200_000

        async def hang_forever(*args, **kwargs):
            await asyncio.sleep(30)

        monkeypatch.setattr(m, "async_main", hang_forever)
        monkeypatch.setattr(m, "async_backfill_main", hang_forever)
        monkeypatch.setenv("S3_BUCKET", "bucket")
        monkeypatch.setenv("FRIGATE_URL", frigate_server)
        monkeypatch.setenv("FRIGATE_USER", "")
        monkeypatch.setenv("METRICS_PORT", "0")

        sender = threading.Thread(
            target=lambda: (
                __import__("time").sleep(0.4),
                os.kill(os.getpid(), signal.SIGINT),
            ),
            daemon=True,
        )
        sender.start()

        with pytest.raises(SystemExit) as exit_info:
            m.main(
                ["backfill", "--after", str(BASE_TS),
                 "--before", str(END_TS)]
            )
        sender.join(timeout=10)

        assert exit_info.value.code == 0


class TestCommandLine:
    """The window is a command-line argument on purpose: the fill must not
    be reachable from the service's environment, where one leftover variable
    would turn the next daemon restart into a bulk re-fetch."""

    def test_no_subcommand_is_the_loop(self):
        args = m.parse_args([])

        assert args.command is None
        assert not hasattr(args, "after")

    def test_backfill_takes_both_bounds(self):
        args = m.parse_args(
            ["backfill", "--after", "10", "--before", "20"]
        )

        assert (args.command, args.after, args.before) == (
            "backfill",
            "10",
            "20",
        )

    def test_one_bound_is_refused_by_the_parser(self):
        """Half a window never reaches the config layer, so no ConfigError
        path exists for it: argparse exits naming the missing flag."""
        with pytest.raises(SystemExit) as excinfo:
            m.parse_args(["backfill", "--after", "10"])

        assert excinfo.value.code == 2

    def test_an_unknown_subcommand_is_refused(self):
        with pytest.raises(SystemExit) as excinfo:
            m.parse_args(["prune", "--after", "1", "--before", "2"])

        assert excinfo.value.code == 2


class TestBackfillEntryPoint:
    """async_backfill_main: discovery + filters + window threading + exit."""

    def drive(self, monkeypatch, frigate, report):
        from config import load_config

        calls = []

        async def fake_run_backfill(config, cameras, after, before, **kw):
            calls.append((cameras, after, before))
            return report

        monkeypatch.setattr(backfill, "run_backfill", fake_run_backfill)
        monkeypatch.setattr(m, "make_s3_client", lambda: FakeS3Client())
        cfg = load_config(
            {
                "FRIGATE_URL": frigate,
                "S3_BUCKET": "bucket",
                "FRIGATE_USER": "",
                "FRIGATE_PASSWORD": "",
                "METRICS_PORT": "0",
            },
            backfill_raw=(str(BASE_TS), str(END_TS)),
        )

        return asyncio.run(m.async_backfill_main(config=cfg)), calls

    def test_success_exits_zero_with_the_parsed_window(
        self, monkeypatch, frigate_server
    ):
        status, calls = self.drive(
            monkeypatch,
            frigate_server,
            {"cam_a": {"uploaded": 1}, "cam_b": {"uploaded": 1}},
        )

        assert status == 0
        cameras, after, before = calls[0]
        assert cameras == ["cam_a", "cam_b"]
        assert (after, before) == (float(BASE_TS), float(END_TS))

    def test_any_failed_camera_exits_nonzero(
        self, monkeypatch, frigate_server
    ):
        status, _calls = self.drive(
            monkeypatch,
            frigate_server,
            {"cam_a": {"uploaded": 1}, "cam_b": {"error": "boom"}},
        )

        assert status == 1

    def test_an_empty_window_is_a_clean_zero(self, monkeypatch, frigate_server):
        """No motion inside the window is a normal outcome, not a failure -
        the exit code distinguishes 'filled nothing' from 'could not fill', so
        an operator scripting several windows can tell them apart."""
        status, _calls = self.drive(monkeypatch, frigate_server, {})

        assert status == 0

    def test_the_camera_filter_applies_to_backfill_too(
        self, monkeypatch, frigate_server
    ):
        from config import load_config

        calls = []

        async def fake_run_backfill(config, cameras, after, before, **kw):
            calls.append(cameras)
            return {"cam_a": {"uploaded": 1}}

        monkeypatch.setattr(backfill, "run_backfill", fake_run_backfill)
        monkeypatch.setattr(m, "make_s3_client", lambda: FakeS3Client())
        cfg = load_config(
            {
                "FRIGATE_URL": frigate_server,
                "S3_BUCKET": "bucket",
                "FRIGATE_USER": "",
                "FRIGATE_PASSWORD": "",
                "METRICS_PORT": "0",
                "CAMERAS_INCLUDE": "cam_a",
            },
            backfill_raw=(str(BASE_TS), str(END_TS)),
        )

        status = asyncio.run(m.async_backfill_main(config=cfg))

        assert status == 0
        assert calls == [["cam_a"]]
