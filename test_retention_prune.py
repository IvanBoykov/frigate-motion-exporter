"""Tests for the prune pass, its supervisor, and the metrics it publishes.

The S3 double is the one from ``test_retention`` - the pass needs a bucket that
answers a real paginator and a real delete, which is exactly what that double
already is.
"""
import asyncio

import pytest
from botocore.exceptions import ClientError
from prometheus_client import REGISTRY
from zoneinfo import ZoneInfo

import archive_census
import frigate_s3_archiver as archiver
from test_frigate_s3_archiver import frigate_server
import metrics
import retention
import retention_prune as rp
from test_retention import FakeS3, build, key

TZ = ZoneInfo("UTC")
PREFIX = "home/"
import test_retention as tr

# Just after the fixture keys' newest hour, so a watermark lookback of a day
# finds them and a cap of a day still bites.
NOW_TS = (
    tr.BASE.replace(hour=6).timestamp()
)

COUNTER = "prune_failures_total"


def sample(labels, name=COUNTER):
    return REGISTRY.get_sample_value(name, labels)


NAMES = iter(
    f"c{i}{j}" for i in range(1000) for j in "abcdefgh"
)


def fake_s3(hours=60, per_hour=4):
    """A two-camera bucket whose camera labels no other test can see.

    Camera labels are unique per test because the prometheus registry is
    process-global: two tests sharing "a" would read each other's gauges and
    both would pass or fail for the wrong reason.
    """
    return build({next(NAMES): hours, next(NAMES): hours}, per_hour=per_hour)


def cameras_of(s3):
    return sorted({k.split("/")[1] for k in s3.objects})


async def run_pass(s3, dry_run=True, keep_days=1, keep_bytes=None, **kw):
    return await rp.run_prune_pass(
        s3,
        "bucket",
        prefix=PREFIX,
        key_timezone=TZ,
        keep_days=keep_days,
        keep_bytes=keep_bytes,
        prune_interval_seconds=kw.pop(
            "prune_interval_seconds",
            retention.DEFAULT_RETENTION_PRUNE_INTERVAL_SECONDS,
        ),
        dry_run=dry_run,
        **kw,
    )


class TestDryRun:
    """Dry run is the default, so its behavior is the behavior an operator sees
    first - including what it reports, which is the point of running one."""

    def test_default_is_dry_run_and_deletes_nothing(self):
        s3 = fake_s3()
        before = dict(s3.objects)

        plan = asyncio.run(run_pass(s3))

        assert plan.folders
        assert s3.deletes == []
        assert s3.objects == before

    def test_dry_run_publishes_the_horizon_it_would_move_to(self):
        """The horizon gauge describes the plan, not the bucket.

        A dry run exists so an operator can review what deletion would do; a
        gauge that kept reporting the about-to-be-deleted footage would leave
        nothing to review, and an alert on archive depth would look healthy.
        """
        s3 = fake_s3()
        camera = cameras_of(s3)[0]

        plan = asyncio.run(run_pass(s3, keep_days=1))

        after = REGISTRY.get_sample_value(
            "archive_horizon_timestamp_seconds", {"camera": camera}
        )
        assert after == plan.cameras[camera].horizon_epoch
        # The plan trims, so the horizon it reports is newer than the oldest
        # hour still sitting in the bucket.
        oldest_in_bucket = min(
            folder
            for folder in s3.objects
            if folder.split("/")[1] == camera
        )
        assert after > archive_census.classify(
            oldest_in_bucket, PREFIX, archive_census.build_pattern(TZ), TZ
        )[3]
        assert REGISTRY.get_sample_value("retention_dry_run") == 1

    def test_dry_run_does_not_count_deletes(self):
        s3 = fake_s3()

        asyncio.run(run_pass(s3))

        for camera in cameras_of(s3):
            assert REGISTRY.get_sample_value(
                "clips_deleted_total", {"camera": camera}
            ) in (None, 0)
            assert REGISTRY.get_sample_value(
                "deleted_bytes_total", {"camera": camera}
            ) in (None, 0)


class TestDeletion:
    def test_refused_keys_are_counted_by_their_s3_code(self):
        """A pass that deleted most of its plan and was refused the rest must
        show the refusals in Prometheus, not only in the log.

        The heartbeat still calls such a pass a success, so without this
        counter the only trace of a permanent delete refusal (credentials that
        can list and write but not delete) is a log line nobody reads.
        """
        s3 = fake_s3()
        victims = [
            k for k in sorted(s3.objects) if "/2026/10/01/00/" in k
        ]
        assert victims
        s3.fail_keys.update(victims)
        camera = victims[0].split("/")[1]

        asyncio.run(run_pass(s3, dry_run=False, keep_days=1))

        # the fake reports InternalError; the label follows the reported code
        refused = REGISTRY.get_sample_value(
            "retention_delete_failures_total",
            {"kind": "s3_internalerror"},
        ) or 0
        assert refused >= len(victims)
        # the refused keys are still in the bucket - retried by the next pass
        assert all(k in s3.objects for k in victims)
        # and they were never charged as deleted
        deleted = REGISTRY.get_sample_value(
            "clips_deleted_total", {"camera": camera}
        ) or 0
        assert deleted <= len(s3.deletes)

    def test_enabled_run_deletes_and_counts(self):
        s3 = fake_s3()

        plan = asyncio.run(run_pass(s3, dry_run=False, keep_days=1))

        assert plan.folders
        assert s3.deletes
        deleted_keys = sum(
            REGISTRY.get_sample_value("clips_deleted_total", {"camera": c})
            or 0
            for c in cameras_of(s3)
        )
        deleted_bytes = sum(
            REGISTRY.get_sample_value("deleted_bytes_total", {"camera": c})
            or 0
            for c in cameras_of(s3)
        )
        assert deleted_keys and deleted_keys > 0
        assert deleted_bytes and deleted_bytes > 0
        assert REGISTRY.get_sample_value("retention_dry_run") == 0

    def test_passes_converge_on_the_floor(self):
        """Days-only retention walks the bucket down to the floor and stops.

        The first pass stops short of the floor because the newest hours are
        held back for growth headroom, so convergence takes two passes; what
        must not happen is a third pass that keeps eating into the archive.
        """
        s3 = fake_s3()
        plans = []
        for _ in range(12):
            plan = asyncio.run(run_pass(s3, dry_run=False, keep_days=1))
            plans.append(plan)
            if not plan.folders:
                break

        assert plans[-1].folders == ()
        assert len(plans) < 12
        settled = set(s3.objects)
        # The floor is counted over full YYYY/MM/DD/HH paths: the HH component
        # repeats every day, so counting it alone would read 24 folders as 24
        # hours across any span.
        for camera in cameras_of(s3):
            hours = {
                "/".join(k.split("/")[2:6])
                for k in settled
                if k.split("/")[1] == camera
            }
            assert len(hours) == 24


class TestCensusFailure:
    def test_an_unreadable_bucket_deletes_nothing_and_is_counted(self):
        class Broken(FakeS3):
            def get_paginator(self, operation_name):
                raise ClientError(
                    {"Error": {"Code": "AccessDenied"}}, "ListObjectsV2"
                )

        s3 = Broken()

        with pytest.raises(ClientError):
            asyncio.run(run_pass(s3, dry_run=False))

        assert s3.deletes == []
        assert REGISTRY.get_sample_value(COUNTER, {"kind": "s3_accessdenied"})

    def test_in_progress_returns_to_zero_after_a_failure(self):
        class Broken(FakeS3):
            def get_paginator(self, operation_name):
                raise RuntimeError("no listing")

        with pytest.raises(RuntimeError):
            asyncio.run(run_pass(Broken(), dry_run=False))

        assert REGISTRY.get_sample_value("prune_in_progress") == 0

    def test_an_unsorted_endpoint_deletes_nothing_and_is_counted(self):
        """An endpoint that does not sort cannot be trusted with the floor, so
        the pass must stop before the first delete and say so under its own
        error kind."""
        s3 = tr.build_unsorted({next(NAMES): 60})

        with pytest.raises(archive_census.ListingOrderError):
            asyncio.run(run_pass(s3, dry_run=False))

        assert s3.deletes == []
        assert REGISTRY.get_sample_value(
            COUNTER, {"kind": "s3_listing_unsorted"}
        )
        assert len(s3.objects) == 60 * 4


class TestGrowthHeadroomSwitch:
    """`RETENTION_GROWTH_HEADROOM_BYTES=0` is the operator's off switch for
    reserving growth room: without it, the target is `current + estimate`, and
    a fat newest hour can make that estimate exceed the target by itself, which
    stops every deletion for an unbounded time."""

    def _spiky_bucket(self):
        """One camera, an ordinary archive, and a newest hour that is 10x an
        ordinary one - a holiday hour, a cat that never left. The size target
        is well above the archive minus the spike but below spike + estimate,
        the exact state that pins the switch down."""
        cam = next(NAMES)
        s3 = build({cam: 40}, per_hour=2, clip_bytes=50 * 2**20)
        newest = max(key(cam, h) for h in range(40))
        folder = newest.rsplit("/", 2)[0]
        for k in list(s3.objects):
            if k.startswith(folder):
                s3.objects[k] = 10 * 2**30
        return s3

    def test_zero_disables_the_reservation_and_deletion_proceeds(self):
        s3 = self._spiky_bucket()

        plan = asyncio.run(
            run_pass(
                s3,
                dry_run=False,
                keep_days=0,
                keep_bytes=8 * 2**30,
                growth_headroom_bytes=0,
                max_delete_percent=100,
            )
        )

        assert plan.reason != retention.REASON_HEADROOM
        assert plan.folders
        assert s3.deletes
        # the published gauge says so too: 0 headroom is visible in the metrics
        assert REGISTRY.get_sample_value("retention_headroom_bytes") == 0

    def test_the_estimate_would_have_stopped_the_same_bucket(self):
        """Without the switch the same bucket deletes nothing, and the reason
        is headroom - the pin that the off switch changes behavior rather
        than passing a knob nobody reads."""
        s3 = self._spiky_bucket()

        plan = asyncio.run(
            run_pass(
                s3,
                dry_run=True,
                keep_days=0,
                keep_bytes=8 * 2**30,
                max_delete_percent=100,
            )
        )

        assert plan.reason == retention.REASON_HEADROOM
        assert plan.folders == ()
        assert (
            REGISTRY.get_sample_value("retention_headroom_bytes") > 8 * 2**30
        )


class TestSupervisor:
    def test_success_waits_the_interval_and_failures_back_off(self):
        slept = []
        outcomes = [RuntimeError("a"), RuntimeError("b"), None]

        async def pass_fn():
            if not outcomes:
                # End the forever-loop. The fake sleep never yields, so a
                # pass that failed instead of succeeding here would retry
                # forever rather than reach the interval assertion.
                raise asyncio.CancelledError
            outcome = outcomes.pop(0)
            if outcome is not None:
                raise outcome

        async def sleep(seconds):
            slept.append(seconds)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(
                rp.supervise_prune(
                    pass_fn,
                    interval_seconds=900,
                    backoff_seconds=10,
                    backoff_max_seconds=60,
                    sleep=sleep,
                )
            )

        assert slept == [10, 20, 900]

    def test_backoff_saturates(self):
        assert rp.restart_delay(1, 30, 1800) == 30
        assert rp.restart_delay(2, 30, 1800) == 60
        assert rp.restart_delay(7, 30, 1800) == 1800
        assert rp.restart_delay(5000, 30, 1800) == 1800

    def test_cancellation_is_not_retried(self):
        async def pass_fn():
            raise asyncio.CancelledError

        async def sleep(seconds):
            raise AssertionError("a cancelled pass must not sleep and retry")

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(rp.supervise_prune(pass_fn, 60, sleep=sleep))


class TestGrowthHours:
    def test_one_pass_interval_needs_one_more_hour_than_it_spans(self):
        assert rp.growth_hours(2700) == 2
        assert rp.growth_hours(3600) == 2
        assert rp.growth_hours(7200) == 3
        assert rp.growth_hours(86400) == 25

    def test_a_zero_interval_is_rejected(self):
        with pytest.raises(ValueError):
            rp.growth_hours(0)


class TestUploadDepthCap:
    """The cap on cold-start depth comes from the config alone, never from a
    prune pass's view of the bucket."""

    def cfg(self, **overrides):
        from config import load_config

        env = {"S3_BUCKET": "bucket", "S3_PREFIX": "home/"}
        env.update({k: str(v) for k, v in overrides.items()})
        return load_config(environ=env)

    def test_cap_follows_the_day_threshold(self):
        cfg = self.cfg(RETENTION_KEEP_DAYS=7)

        assert archiver.upload_age_cap_seconds(cfg) == 7 * 24 * 3600

    def test_no_retention_means_no_cap(self):
        assert archiver.upload_age_cap_seconds(self.cfg()) is None

    def test_a_size_target_does_not_move_the_cap(self):
        """The cap is a depth, so only the day threshold can set it; the byte
        target says what the archive may weigh, not how far a restart looks."""
        cfg = self.cfg(
            RETENTION_KEEP_DAYS=30, RETENTION_KEEP_BYTES=1024**3
        )

        assert archiver.upload_age_cap_seconds(cfg) == 30 * 24 * 3600

    def test_the_cap_can_be_switched_off(self):
        cfg_off = self.cfg(
            RETENTION_KEEP_DAYS=30,
            RETENTION_KEEP_BYTES=1024**3,
            RETENTION_LIMIT_UPLOAD_DEPTH="false",
        )

        assert archiver.upload_age_cap_seconds(cfg_off) is None

    def test_no_day_floor_means_no_cap(self):
        """KEEP_DAYS=0 asks for no age floor, so nothing a cold start archives
        is doomed and the loader keeps its full FIRST_RUN_LOOKBACK_SECONDS."""
        cfg = self.cfg(RETENTION_KEEP_DAYS=0, RETENTION_KEEP_BYTES=1024**3)

        assert archiver.upload_age_cap_seconds(cfg) is None

    def test_a_cold_start_clamped_by_the_cap_reports_the_cap(self):
        s3 = FakeS3()

        start = asyncio.run(archiver.resolve_camera_watermark(
            "cam",
            "bucket",
            s3,
            now_ts=NOW_TS,
            key_timezone=TZ,
            first_run_lookback_seconds=3 * 24 * 3600,
            max_upload_age_seconds=1 * 24 * 3600,
        ))

        assert start == NOW_TS - 1 * 24 * 3600

    def test_a_resume_is_never_clamped(self):
        """An existing watermark is the archive's own boundary; clamping it
        would move it backwards and re-archive footage already stored."""
        newest = key("cam", 2)
        s3 = FakeS3({newest: 10})

        start = asyncio.run(archiver.resolve_camera_watermark(
            "cam",
            "bucket",
            s3,
            now_ts=NOW_TS,
            key_timezone=TZ,
            watermark_lookback_seconds=30 * 24 * 3600,
            first_run_lookback_seconds=3 * 24 * 3600,
            max_upload_age_seconds=1 * 24 * 3600,
            key_prefix=PREFIX,
        ))

        expected = archive_census.classify(
            newest, PREFIX, archive_census.build_pattern(TZ), TZ
        )[3]
        # The clamp would have produced NOW_TS minus its own cap, which is
        # newer than the stored clip; a first-run start would be NOW_TS minus a
        # lookback. Neither equals the clip start, so this distinguishes all
        # three branches and proves the resume path is the one taken.
        assert start == float(expected)


class LoggedFakeS3(FakeS3):
    """The runtime path both registers a botocore retry hook and logs the
    endpoint, so the double needs the events registrar and the two meta fields
    that log reads - the plain FakeS3 has only the first."""

    class _Events:
        @staticmethod
        def register(event_name, handler):
            pass

    class _ClientMeta:
        endpoint_url = "https://s3.test.internal"
        region_name = "us-east-1"
        events = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._meta = LoggedFakeS3._ClientMeta()
        self._meta.events = LoggedFakeS3._Events()

    @property
    def meta(self):
        return self._meta


class TestRuntimeWiring:
    """The prune task must be started by the loop that starts the camera tasks,
    with the config's thresholds and the age cap threaded through. Unit tests
    of run_prune_pass cannot show that the wiring exists."""

    def drive(self, monkeypatch, frigate_server, extra_env):
        import asyncio
        from config import load_config
        import retention_prune as rp

        cameras = {}
        pruners = []

        async def fake_run_camera(camera, **kwargs):
            cameras[camera] = kwargs
            await asyncio.Event().wait()

        async def fake_run_pruner(s3_client, config, key_timezone, now_fn=None):
            pruners.append(config)
            await asyncio.Event().wait()

        env = {
            "FRIGATE_URL": frigate_server,
            "S3_BUCKET": "bucket",
            "FRIGATE_USER": "",
            "FRIGATE_PASSWORD": "",
            "METRICS_PORT": "0",
            **extra_env,
        }
        monkeypatch.setattr(
            archiver.boto3,
            "client",
            lambda name, **kw: LoggedFakeS3(),
        )
        monkeypatch.setattr(archiver, "run_camera", fake_run_camera)
        monkeypatch.setattr(rp, "run_pruner", fake_run_pruner)

        async def scenario(cfg):
            task = asyncio.create_task(
                archiver.async_main(config=cfg, now_fn=lambda: NOW_TS)
            )
            for _ in range(400):
                if cameras and (
                    not cfg.retention_enabled or pruners
                ):
                    break
                await asyncio.sleep(0.01)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        cfg = load_config(environ=env)
        asyncio.run(scenario(cfg))
        return cameras, pruners

    def test_retention_starts_a_prune_task(self, monkeypatch, frigate_server):
        cameras, pruners = self.drive(
            monkeypatch,
            frigate_server,
            {"RETENTION_KEEP_DAYS": "7"},
        )
        assert set(cameras) == {"cam_a", "cam_b"}
        assert len(pruners) == 1
        assert pruners[0].retention_keep_days == 7

    def test_no_retention_starts_no_prune_task(self, monkeypatch, frigate_server):
        cameras, pruners = self.drive(monkeypatch, frigate_server, {})
        assert set(cameras) == {"cam_a", "cam_b"}
        assert pruners == []

    def test_the_age_cap_reaches_every_camera_task(
        self, monkeypatch, frigate_server
    ):
        """KEEP_DAYS must reach the camera tasks, not just the pruner: the cap
        is what keeps a cold restart from re-archiving what deletion removes."""
        cameras, _ = self.drive(
            monkeypatch,
            frigate_server,
            {"RETENTION_KEEP_DAYS": "7"},
        )
        for kwargs in cameras.values():
            assert kwargs["max_upload_age_seconds"] == 7 * 24 * 3600

    def test_the_age_cap_is_absent_without_retention(
        self, monkeypatch, frigate_server
    ):
        cameras, _ = self.drive(monkeypatch, frigate_server, {})
        for kwargs in cameras.values():
            assert kwargs["max_upload_age_seconds"] is None
