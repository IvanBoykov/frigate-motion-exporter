"""Retention: the census, the plan, and the delete executor.

The objects here are the real ones - a real fake S3 that stores keys and sizes
and answers a real paginator, and the real planner and executor. What is under
test is the arithmetic that decides what disappears, so the double has to be
faithful about the two properties that arithmetic leans on: listings come back
in lexicographic order, and a page holds a bounded number of keys.
"""
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from botocore.exceptions import ClientError

import archive_census as census
import frigate_s3_archiver as archiver
import retention as rt
import retention_delete as rd

TZ = ZoneInfo("UTC")
PREFIX = "home/"
BASE = datetime(2026, 10, 1, 0, 0, tzinfo=TZ)


def key(camera, hours_offset, index=0, suffix="", extension=".mp4"):
    """One key ``hours_offset`` hours after BASE, in the layout we write.

    ``index`` moves the minute/second inside the hour so a fixture with several
    clips per hour folder gets distinct keys, as the real layout does.
    """
    moment = BASE + timedelta(hours=hours_offset, minutes=index)
    return (
        f"{PREFIX}{camera}/"
        f"{moment:%Y/%m/%d/%H}/"
        f"{moment:%M-%S}{suffix}{extension}"
    )


class FakeS3:
    """Keys with sizes, lexicographic listings, and page-limited responses."""

    class _Meta:
        class events:
            @staticmethod
            def register(event_name, handler):
                pass

    meta = _Meta()

    def __init__(self, objects=None, page_size=1000, batch_supported=True):
        self.objects = dict(objects or {})
        self.page_size = page_size
        self.deletes = []
        self.listings = []
        self.fail_keys = set()
        self.unsupported_error = not batch_supported
        self._calls = []

    def put(self, key, size):
        self.objects[key] = size

    def head_object(self, Bucket, Key):
        if Key in self.objects:
            return {"ContentLength": self.objects[Key]}
        raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        return FakePaginator(self)

    def delete_object(self, Bucket, Key):
        self._calls.append(("delete_object", Key))
        self.deletes.append(Key)
        self.objects.pop(Key, None)
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def delete_objects(self, Bucket, Delete, Quiet=False):
        self._calls.append(("delete_objects", len(Delete["Objects"])))
        if self.unsupported_error:
            raise ClientError(
                {"Error": {"Code": "NotImplemented"}}, "DeleteObjects"
            )
        errors = []
        for spec in Delete["Objects"]:
            key = spec["Key"]
            self.deletes.append(key)
            if key in self.fail_keys:
                errors.append({"Key": key, "Code": "InternalError"})
                continue
            self.objects.pop(key, None)
        return {"Deleted": [], "Errors": errors}


class FakePaginator:
    def __init__(self, s3):
        self.s3 = s3

    def paginate(self, Bucket, Prefix=""):
        keys = sorted(k for k in self.s3.objects if k.startswith(Prefix))
        if not keys:
            self.s3.listings.append(Prefix)
            yield {"Contents": [], "KeyCount": 0}
            return
        for offset in range(0, len(keys), self.s3.page_size):
            chunk = keys[offset : offset + self.s3.page_size]
            self.s3.listings.append(Prefix)
            yield {
                "Contents": [
                    {"Key": key, "Size": self.s3.objects[key]}
                    for key in chunk
                ],
                "KeyCount": len(chunk),
            }


def build(
    rooms,
    per_hour=4,
    clip_bytes=1024,
    start_hour=0,
    page_size=1000,
    batch_supported=True,
):
    """``{camera: hours}`` filled with evenly spaced clips.

    ``rooms`` maps a camera to how many hour folders it has, so a two-camera
    fixture with a different depth per camera is one dict away. Sizes vary by
    hour and index so no two folders tie.
    """
    s3 = FakeS3(page_size=page_size, batch_supported=batch_supported)
    for camera, hours in rooms.items():
        for hour in range(start_hour, start_hour + hours):
            for index in range(per_hour):
                s3.put(
                    key(camera, hour, index),
                    clip_bytes + hour + index,
                )
    return s3


def take(s3, floor_hours=0, recent_hours=0, prefix=PREFIX):
    return census.take_census(
        s3,
        "bucket",
        prefix=prefix,
        key_timezone=TZ,
        floor_hours=floor_hours,
        recent_hours=recent_hours,
    )


def folders_of(plan, camera=None):
    return [
        folder
        for folder in plan.folders
        if camera is None or folder.camera == camera
    ]


# ---------------------------------------------------------------- census


def test_census_accounts_for_every_byte_under_the_prefix():
    s3 = build({"a": 5, "b": 3})
    expected = sum(s3.objects.values())
    result = take(s3, floor_hours=24)

    candidates = sum(
        folder.bytes for row in result.hours.values() for folder in row.values()
    )
    protected = sum(s.protected_bytes for s in result.cameras.values())
    assert result.total_bytes == expected
    assert candidates + protected + result.unknown_bytes == expected


def test_census_only_sees_the_prefix_and_reports_foreign_keys_inside_it():
    """Keys outside the prefix are not foreign, they are invisible.

    The listing is prefix-scoped, so the archive's size is the prefix's size
    and data elsewhere in the bucket is nobody's budget. What the census has to
    catch is a foreign key *inside* the prefix: a changed S3_PREFIX or
    S3_KEY_TIMEZONE makes our own older archive look exactly like this.
    """
    s3 = build({"a": 2})
    outside = "other/camera/2026-10-01/00/00-00.mp4"
    s3.put(outside, 999)
    s3.put(PREFIX + "notes.txt", 7)
    result = take(s3, floor_hours=24)

    assert result.unknown_keys == 1
    assert result.unknown_bytes == 7
    assert result.total_bytes == sum(
        size for k, size in s3.objects.items() if k.startswith(PREFIX)
    )

def test_floor_hours_never_appear_as_candidates():
    """The protected tail is absent from the candidate map, not just unused.

    This is the property that makes the memory shape possible, and the one that
    inverts if the threshold is applied on the way in rather than on eviction:
    listings arrive oldest-first, so an hour seen while the counter is still
    below the floor is one of the oldest, not one of the newest.
    """
    s3 = build({"a": 30})
    result = take(s3, floor_hours=24)

    assert result.cameras["a"].protected_hours == 24
    candidates = sorted(
        folder.epoch for row in result.hours.values() for folder in row.values()
    )
    assert candidates == [
        int((BASE + timedelta(hours=hour)).timestamp()) for hour in range(6)
    ]
    assert result.cameras["a"].protected_oldest_epoch == int(
        (BASE + timedelta(hours=6)).timestamp()
    )


def test_a_camera_shorter_than_the_floor_is_fully_protected():
    result = take(build({"a": 3}), floor_hours=24 * 7)

    assert folders_of(rt.build_plan(result, keep_bytes=None)) == []
    assert result.cameras["a"].protected_hours == 3
    assert result.hours == {}


def test_floor_counts_hours_not_elapsed_time():
    """A sparse camera keeps N hours' worth of folders over a long span.

    The threshold is a count of recordings, so gaps in the recording extend the
    calendar span the floor covers instead of shrinking it.
    """
    s3 = FakeS3()
    for day in range(40):
        s3.put(key("a", day * 24), 100)
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)

    assert result.cameras["a"].protected_hours == 24
    assert len(folders_of(plan)) == 16


def test_closing_markers_and_truncated_clips_are_ours():
    s3 = build({"a": 1})
    s3.put(key("a", 0, suffix=archiver.TRUNCATED_KEY_SUFFIX), 50)
    s3.put(
        key(
            "a",
            0,
            index=9,
            suffix=archiver.UNAVAILABLE_KEY_SUFFIX,
            extension="",
        ),
        0,
    )
    result = take(s3)

    assert result.unknown_keys == 0
    assert result.total_keys == 6


def test_census_pages_through_a_bucket_larger_than_one_page():
    s3 = build({"a": 40}, per_hour=10, page_size=64)
    result = take(s3, floor_hours=24)

    assert result.total_keys == 400
    assert len(s3.listings) >= 7
    candidate_keys = sum(
        folder.keys for row in result.hours.values() for folder in row.values()
    )
    assert candidate_keys == 160
    assert result.cameras["a"].protected_hours == 24

def test_growth_headroom_sums_the_newest_hours_over_all_cameras():
    result = take(build({"a": 10, "b": 10}), recent_hours=2)
    newest_of_a = result.cameras["a"].recent_hour_bytes[-1]
    newest_of_b = result.cameras["b"].recent_hour_bytes[-1]

    assert census.growth_headroom(result, 1) == newest_of_a + newest_of_b
    assert census.growth_headroom(result, 2) > census.growth_headroom(
        result, 1
    )
    with pytest.raises(ValueError):
        census.growth_headroom(result, 0)


def test_growth_headroom_extrapolates_a_camera_with_fewer_hours():
    """A camera newer than the window still contributes a full estimate."""
    result = take(build({"a": 1}), recent_hours=4)
    only_hour = result.cameras["a"].recent_hour_bytes[0]

    assert census.growth_headroom(result, 4) == only_hour * 4


# ---------------------------------------------------------------- plan


def test_days_only_removes_exactly_what_the_floor_leaves():
    result = take(build({"a": 40, "b": 40}), floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)

    assert len(folders_of(plan)) == 32
    assert {folder.camera for folder in plan.folders} == {"a", "b"}
    assert plan.reason == rt.REASON_TARGET_MET
    assert plan.over_budget_bytes == 0


def test_the_floor_is_never_touched_however_small_the_target():
    result = take(build({"a": 60, "b": 60}), floor_hours=24)
    plan = rt.build_plan(
        result,
        keep_bytes=1,
        headroom_bytes=0,
        max_delete_percent=100,
    )

    floor_bytes = sum(s.protected_bytes for s in result.cameras.values())
    assert result.total_bytes - plan.freed_bytes == floor_bytes
    assert plan.reason == rt.REASON_FLOOR
    assert plan.over_budget_bytes > 0


def test_target_means_nothing_is_deleted():
    result = take(build({"a": 40}), floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=result.total_bytes)

    assert plan.folders == ()
    assert plan.reason == rt.REASON_TARGET_MET
    assert sum(r.over_budget_bytes for r in plan.cameras.values()) == 0


def test_headroom_is_part_of_the_target():
    """A target equal to the archive size still deletes, because growth counts.

    The target has to leave room for the interval's writes or a quota-sized
    bucket overflows on the next pass.
    """
    result = take(build({"a": 40}), floor_hours=24)
    headroom = 4096

    without = rt.build_plan(result, keep_bytes=result.total_bytes)
    with_headroom = rt.build_plan(
        result, keep_bytes=result.total_bytes, headroom_bytes=headroom
    )

    assert without.folders == ()
    assert with_headroom.freed_bytes > 0


def test_horizon_is_the_same_hour_for_every_camera():
    """The fairness property: depth is compared by age, not by folder count.

    Under one sweep the oldest surviving hour must not belong to one camera
    while another has already lost that hour.
    """
    result = take(
        build({"fat": 80, "thin": 80}, clip_bytes=4096),
        floor_hours=24,
    )

    horizons = set()
    for target in (0.9, 0.75, 0.6, 0.45, 0.3):
        plan = rt.build_plan(
            result,
            keep_bytes=int(result.total_bytes * target),
            max_delete_percent=100,
        )
        epochs = [r.horizon_epoch for r in plan.cameras.values()]
        horizons.add(tuple(sorted(epochs)))
        assert max(epochs) - min(epochs) <= 3600, (target, epochs)

    assert len(horizons) > 3


def test_plan_is_monotone_in_the_target():
    result = take(build({"a": 70, "b": 50}), floor_hours=24)
    previous_freed = None

    for fraction in [x / 40 for x in range(5, 41)]:
        plan = rt.build_plan(
            result,
            keep_bytes=int(result.total_bytes * fraction),
            max_delete_percent=100,
        )
        if previous_freed is not None:
            assert plan.freed_bytes <= previous_freed
        previous_freed = plan.freed_bytes


def test_sweep_goes_oldest_first_across_cameras():
    result = take(build({"a": 50, "b": 50}), floor_hours=24)
    plan = rt.build_plan(
        result,
        keep_bytes=int(result.total_bytes * 0.5),
        max_delete_percent=100,
    )
    epochs = [folder.epoch for folder in plan.folders]

    assert epochs == sorted(epochs)
    oldest = min(epochs)
    assert {
        folder.camera for folder in plan.folders if folder.epoch == oldest
    } == {"a", "b"}


def test_large_folders_are_taken_first_within_an_hour():
    s3 = FakeS3()
    s3.put(key("a", 0, 0), 10_000)
    s3.put(key("b", 0, 0), 9_000)
    s3.put(key("a", 100, 0), 1)
    s3.put(key("b", 100, 0), 1)
    result = take(s3, floor_hours=0)
    plan = rt.build_plan(result, keep_bytes=1, max_delete_percent=100)

    assert [folder.camera for folder in plan.folders] == ["a", "b"]


def test_percent_cap_brakes_rather_than_refusing():
    result = take(build({"a": 60, "b": 60}), floor_hours=24)
    plan = rt.build_plan(
        result,
        keep_bytes=1,
        max_delete_percent=10,
    )
    cap = int(result.total_bytes * 10 / 100)

    assert plan.freed_bytes <= cap
    assert plan.freed_bytes > cap * 0.5
    assert plan.reason == rt.REASON_PERCENT_CAP
    assert plan.over_budget_bytes > 0


def test_foreign_majority_refuses_the_pass():
    result = take(build({"a": 40}), floor_hours=24)
    tainted = result._replace(unknown_bytes=result.total_bytes)

    plan = rt.build_plan(tainted, keep_bytes=1, max_delete_percent=100)

    assert plan.folders == ()
    assert plan.reason == rt.REASON_FOREIGN_KEYS


def test_a_little_foreign_data_does_not_disable_retention():
    result = take(build({"a": 40}), floor_hours=24)
    tainted = result._replace(
        total_bytes=result.total_bytes + 1024,
        unknown_bytes=1024,
        unknown_keys=1,
    )

    plan = rt.build_plan(tainted, keep_bytes=None, max_delete_percent=100)

    assert len(plan.folders) == 16


def test_newest_hour_survives_without_a_day_floor():
    """Size-only mode: the newest clip is what the watermark resumes from."""
    result = take(build({"a": 40}), floor_hours=0)
    plan = rt.build_plan(result, keep_bytes=1, max_delete_percent=100)
    newest = result.cameras["a"].newest_epoch

    assert folders_of(plan)
    assert all(folder.epoch != newest for folder in plan.folders)


def test_plan_reports_a_horizon_per_camera_after_the_plan():
    result = take(build({"a": 40}), floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    report = plan.cameras["a"]

    assert report.horizon_epoch == result.cameras["a"].protected_oldest_epoch
    assert report.protected_hours == 24


# ------------------------------------------------------------ executor


def test_executor_deletes_exactly_the_planned_folders():
    s3 = build({"a": 40, "b": 40})
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    before = dict(s3.objects)
    planned_keys = {
        key
        for folder in plan.folders
        for key in before
        if key.startswith(folder.folder_key)
    }

    per_camera, freed, failed = rd.execute_plan(
        s3, "bucket", plan, prefix=PREFIX, key_timezone=TZ
    )

    assert failed == []
    assert set(s3.deletes) == planned_keys
    assert set(s3.objects) == set(before) - planned_keys
    assert freed == sum(before[k] for k in planned_keys)
    assert sum(count for count, _ in per_camera.values()) == len(planned_keys)



def test_executor_touches_no_key_outside_the_planned_folders():
    """Folder-granular, and scoped to the plan.

    A key written into a planned folder after the census is below the floor by
    that folder's own age, so it goes with the folder - that is what "this hour
    is gone" means. The property worth pinning is the scope: the executor
    cannot reach a folder the plan did not name, which is what keeps a wrong
    plan from becoming a wider deletion.
    """
    s3 = build({"a": 40})
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    planned_folders = {folder.folder_key for folder in plan.folders}
    survivor = next(
        k
        for k in s3.objects
        if not any(k.startswith(f) for f in planned_folders)
    )
    survivor_hour = "/".join(survivor.split("/")[2:6]) + "/"
    before = set(s3.objects)

    rd.execute_plan(s3, "bucket", plan, prefix=PREFIX, key_timezone=TZ)

    assert survivor in s3.objects
    assert not any(d.startswith(survivor_hour) for d in s3.deletes)
    assert set(s3.deletes) <= before
    assert set(s3.objects) == before - set(s3.deletes)
def test_executor_never_deletes_foreign_keys_inside_a_planned_folder():
    s3 = build({"a": 40})
    foreign = PREFIX + "a/2026/10/01/00/hand-written.txt"
    s3.put(foreign, 10)
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)

    rd.execute_plan(s3, "bucket", plan, prefix=PREFIX, key_timezone=TZ)

    assert foreign in s3.objects
    assert foreign not in s3.deletes


def test_executor_relists_a_folder_it_deleted_too_early():
    s3 = build({"a": 40})
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    folder = plan.folders[0].folder_key
    s3.objects = {
        k: v for k, v in s3.objects.items() if not k.startswith(folder)
    }

    per_camera, freed, failed = rd.execute_plan(
        s3, "bucket", plan, prefix=PREFIX, key_timezone=TZ
    )

    assert failed == []
    assert freed >= 0
    assert not any(deleted.startswith(folder) for deleted in s3.deletes)



def test_executor_falls_back_when_batch_delete_is_unsupported():
    """Probed once, then per-key for the rest of the pass.

    The probe itself counts as a call, so the assertion is that exactly one
    batch attempt happened and every key still went.
    """
    s3 = build({"a": 40}, batch_supported=False)
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    planned = sum(folder.keys for folder in plan.folders)

    _per_camera, freed, failed = rd.execute_plan(
        s3, "bucket", plan, prefix=PREFIX, key_timezone=TZ
    )

    batch_attempts = [c for c in s3._calls if c[0] == "delete_objects"]
    singles = [c for c in s3._calls if c[0] == "delete_object"]
    assert failed == []
    assert freed > 0
    assert len(batch_attempts) == 1
    assert len(singles) == planned
def test_batch_errors_are_reported_not_swallowed():
    s3 = build({"a": 40})
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    victim = next(
        key
        for key in s3.objects
        if key.startswith(plan.folders[0].folder_key)
    )
    s3.fail_keys.add(victim)

    _per_camera, _freed, failed = rd.execute_plan(
        s3, "bucket", plan, prefix=PREFIX, key_timezone=TZ
    )

    assert failed == [victim]
    assert victim in s3.objects


def test_cancellation_stops_between_folders():
    s3 = build({"a": 40})
    result = take(s3, floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)
    event = asyncio.Event()
    event.set()

    with pytest.raises(asyncio.CancelledError):
        rd.execute_plan(
            s3,
            "bucket",
            plan,
            prefix=PREFIX,
            key_timezone=TZ,
            cancel_event=event,
        )

    assert s3.deletes == []


def test_delete_is_idempotent_across_two_passes():
    s3 = build({"a": 60})
    plan_one = rt.build_plan(
        take(s3, floor_hours=24), keep_bytes=None, max_delete_percent=100
    )
    rd.execute_plan(s3, "bucket", plan_one, prefix=PREFIX, key_timezone=TZ)
    survivors = set(s3.objects)

    plan_two = rt.build_plan(
        take(s3, floor_hours=24), keep_bytes=None, max_delete_percent=100
    )
    rd.execute_plan(s3, "bucket", plan_two, prefix=PREFIX, key_timezone=TZ)

    assert set(s3.objects) == survivors
    assert plan_two.folders == ()


def test_real_s3_client_shape_is_what_the_executor_calls(monkeypatch):
    """The executor must not grow a method the real client lacks.

    Guards the one thing a fake cannot catch on its own: a typo in a boto3
    call name fails only against a real endpoint, which is a bad way to find
    out during the first deletion on a production bucket.
    """
    calls = []

    class Shape:
        def get_paginator(self, name):
            calls.append(("get_paginator", name))
            return FakePaginator(build({"a": 30}))

        def delete_objects(self, Bucket, Delete, Quiet=False):
            calls.append(("delete_objects", len(Delete["Objects"])))
            return {"Deleted": [], "Errors": []}

    result = take(build({"a": 30}), floor_hours=24)
    plan = rt.build_plan(result, keep_bytes=None, max_delete_percent=100)

    rd.execute_plan(
        Shape(), "bucket", plan, prefix=PREFIX, key_timezone=TZ
    )

    assert ("get_paginator", "list_objects_v2") in calls
    assert any(name == "delete_objects" for name, _ in calls)
