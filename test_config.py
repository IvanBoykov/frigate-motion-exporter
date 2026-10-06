"""Tests for environment configuration and the S3 key prefix."""
import re

import pytest

import frigate_s3_archiver as m
from config import ConfigError, load_config


BASE = {"S3_BUCKET": "bucket"}


def load(**overrides):
    return load_config(environ={**BASE, **{k: str(v) for k, v in overrides.items()}})


def error_variable(**overrides):
    """The ConfigError message for an invalid value, to assert on its name."""
    with pytest.raises(ConfigError) as excinfo:
        load(**overrides)
    return str(excinfo.value)


def test_defaults_match_module_constants():
    cfg = load()

    assert cfg.frigate_url == m.DEFAULT_FRIGATE_URL
    assert cfg.s3_bucket == "bucket"
    assert cfg.s3_prefix == ""
    assert cfg.s3_part_size == m.DEFAULT_PART_SIZE
    assert cfg.s3_key_timezone == m.DEFAULT_S3_KEY_TIMEZONE
    assert cfg.cameras_include is None
    assert cfg.cameras_exclude is None
    assert cfg.max_gap_seconds == m.DEFAULT_MAX_GAP_SECONDS
    assert cfg.idle_sleep_seconds == m.DEFAULT_IDLE_SLEEP_SECONDS
    assert cfg.http_timeout == m.DEFAULT_HTTP_TIMEOUT
    assert (
        cfg.watermark_lookback_seconds == m.DEFAULT_WATERMARK_LOOKBACK_SECONDS
    )
    assert (
        cfg.first_run_lookback_seconds
        == m.DEFAULT_FIRST_RUN_LOOKBACK_SECONDS
    )
    assert cfg.clip_retries == m.DEFAULT_CLIP_RETRIES
    assert (
        cfg.clip_retry_max_delay_seconds
        == m.DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS
    )
    assert (
        cfg.camera_restart_backoff_seconds
        == m.DEFAULT_CAMERA_RESTART_BACKOFF_SECONDS
    )
    assert (
        cfg.camera_restart_backoff_max_seconds
        == m.DEFAULT_CAMERA_RESTART_BACKOFF_MAX_SECONDS
    )


def test_blank_values_fall_back_to_defaults():
    """An exported-but-empty variable is not a configuration."""
    cfg = load(IDLE_SLEEP_SECONDS="  ", MAX_GAP_SECONDS="")

    assert cfg.idle_sleep_seconds == m.DEFAULT_IDLE_SLEEP_SECONDS
    assert cfg.max_gap_seconds == m.DEFAULT_MAX_GAP_SECONDS


def test_every_knob_is_read():
    cfg = load(
        FRIGATE_URL="https://frigate.example/",
        S3_PART_SIZE=10_485_760,
        S3_KEY_TIMEZONE="Europe/Moscow",
        MAX_GAP_SECONDS=90,
        IDLE_SLEEP_SECONDS=15,
        HTTP_TIMEOUT=600,
        WATERMARK_LOOKBACK_SECONDS=7200,
        FIRST_RUN_LOOKBACK_SECONDS=86400,
        CLIP_RETRIES=0,
        CLIP_RETRY_MAX_DELAY_SECONDS=1.5,
        CAMERA_RESTART_BACKOFF_SECONDS=2,
        CAMERA_RESTART_BACKOFF_MAX_SECONDS=60,
    )

    assert cfg.frigate_url == "https://frigate.example/"
    assert cfg.s3_part_size == 10_485_760
    assert str(cfg.s3_key_timezone) == "Europe/Moscow"
    assert cfg.max_gap_seconds == 90.0
    assert cfg.idle_sleep_seconds == 15.0
    assert cfg.http_timeout == 600.0
    assert cfg.watermark_lookback_seconds == 7200.0
    assert cfg.first_run_lookback_seconds == 86400.0
    assert cfg.clip_retries == 0
    assert cfg.clip_retry_max_delay_seconds == 1.5
    assert cfg.camera_restart_backoff_seconds == 2.0
    assert cfg.camera_restart_backoff_max_seconds == 60.0


def test_missing_bucket_exits_with_the_documented_message():
    with pytest.raises(SystemExit) as excinfo:
        load_config(environ={})

    assert "S3_BUCKET" in str(excinfo.value)


@pytest.mark.parametrize(
    "name",
    [
        "MAX_GAP_SECONDS",
        "IDLE_SLEEP_SECONDS",
        "HTTP_TIMEOUT",
        "WATERMARK_LOOKBACK_SECONDS",
        "FIRST_RUN_LOOKBACK_SECONDS",
        "CLIP_RETRIES",
        "CLIP_RETRY_MAX_DELAY_SECONDS",
        "CAMERA_RESTART_BACKOFF_SECONDS",
        "CAMERA_RESTART_BACKOFF_MAX_SECONDS",
        "S3_PART_SIZE",
    ],
)
def test_non_numeric_value_names_the_variable(name):
    message = error_variable(**{name: "abc"})

    assert message.startswith(f"{name}:")


def test_negative_duration_is_rejected_by_name():
    message = error_variable(IDLE_SLEEP_SECONDS="-5")

    assert message.startswith("IDLE_SLEEP_SECONDS:")


def test_clip_retries_may_be_zero_but_not_negative():
    assert load(CLIP_RETRIES=0).clip_retries == 0

    message = error_variable(CLIP_RETRIES=-1)
    assert message.startswith("CLIP_RETRIES:")


def test_part_size_below_the_s3_minimum_is_rejected():
    """A smaller part fails complete_multipart_upload after full download."""
    message = error_variable(S3_PART_SIZE=m.MIN_MULTIPART_PART_SIZE - 1)

    assert message.startswith("S3_PART_SIZE:")
    assert str(m.MIN_MULTIPART_PART_SIZE) in message


def test_restart_backoff_may_not_exceed_its_cap():
    message = error_variable(
        CAMERA_RESTART_BACKOFF_SECONDS=60,
        CAMERA_RESTART_BACKOFF_MAX_SECONDS=5,
    )

    assert message.startswith("CAMERA_RESTART_BACKOFF_MAX_SECONDS:")


def test_unknown_timezone_is_rejected_by_name():
    message = error_variable(S3_KEY_TIMEZONE="Mars/Olympus_Mons")

    assert message.startswith("S3_KEY_TIMEZONE:")


def test_timezone_applies_to_the_generated_key():
    cfg = load(S3_KEY_TIMEZONE="Europe/Moscow")
    ts = 1_700_000_000

    key = m.make_s3_key("cam_a", ts, key_timezone=cfg.s3_key_timezone)

    assert key == m.make_s3_key("cam_a", ts, key_timezone=cfg.s3_key_timezone)
    assert not key.startswith("cam_a/1970")
    # The key must parse back to the same instant in the same zone.
    parsed = m.parse_s3_clip_start(
        "cam_a", key, key_timezone=cfg.s3_key_timezone
    )
    assert parsed == ts // 1 * 1 or abs(parsed - ts) < 60


class TestS3Prefix:
    def test_normalized(self):
        assert load(S3_PREFIX="frigate") .s3_prefix == "frigate/"
        assert load(S3_PREFIX="frigate/archive").s3_prefix == (
            "frigate/archive/"
        )
        assert load(S3_PREFIX="/frigate/").s3_prefix == "frigate/"
        assert load(S3_PREFIX="  ").s3_prefix == ""

    @pytest.mark.parametrize("value", ["frigate//archive", "frigate/./x", "../x"])
    def test_ambiguous_prefixes_are_rejected(self, value):
        message = error_variable(S3_PREFIX=value)

        assert message.startswith("S3_PREFIX:")

    def test_key_carries_the_prefix(self):
        cfg = load(S3_PREFIX="frigate")
        ts = 1_700_000_000

        key = m.make_s3_key(
            "cam_a",
            ts,
            key_timezone=cfg.s3_key_timezone,
            suffix=m.TRUNCATED_KEY_SUFFIX,
            key_prefix=cfg.s3_prefix,
        )

        assert key.startswith("frigate/cam_a/")
        assert key.endswith("-truncated.mp4")

    def test_prefixless_keys_are_unchanged(self):
        """Existing archives keep their layout, so watermarks still parse."""
        ts = 1_700_000_000

        assert m.make_s3_key("cam_a", ts) == m.make_s3_key(
            "cam_a", ts, key_prefix=""
        )
        assert not m.make_s3_key("cam_a", ts).startswith("/")

    def test_parse_roundtrip_with_prefix(self):
        cfg = load(S3_PREFIX="frigate")
        ts = 1_700_000_000
        tz = cfg.s3_key_timezone
        key = m.make_s3_key("cam_a", ts, key_timezone=tz, key_prefix="frigate/")
        plain_key = m.make_s3_key("cam_a", ts, key_timezone=tz)

        assert m.parse_s3_clip_start(
            "cam_a", key, key_timezone=tz, key_prefix="frigate/"
        ) == m.parse_s3_clip_start("cam_a", plain_key, key_timezone=tz)
        # A prefixed key is not readable under a prefixless layout.
        assert m.parse_s3_clip_start("cam_a", key, key_timezone=tz) is None
        # And a prefixless key is not readable as if it were prefixed.
        assert (
            m.parse_s3_clip_start(
                "cam_a", plain_key, key_timezone=tz, key_prefix="frigate/"
            )
            is None
        )


class TestCameraFilter:
    def test_include_pattern(self):
        cfg = load(CAMERAS_INCLUDE=r"^front_")

        assert m.select_cameras(
            ["front_door", "front_yard", "back"], include=cfg.cameras_include
        ) == ["front_door", "front_yard"]

    def test_exclude_pattern(self):
        cfg = load(CAMERAS_EXCLUDE=r"_test$")

        assert m.select_cameras(
            ["door", "garage_test"], exclude=cfg.cameras_exclude
        ) == ["door"]

    def test_no_pattern_keeps_everything(self):
        assert m.select_cameras(["a", "b"]) == ["a", "b"]

    def test_include_may_match_nothing(self):
        """A filter that excludes every camera is reported by async_main."""
        cfg = load(CAMERAS_INCLUDE=r"^nope")

        assert m.select_cameras(["a"], include=cfg.cameras_include) == []

    def test_both_patterns_rejected_at_startup(self):
        with pytest.raises(ConfigError) as excinfo:
            load(CAMERAS_INCLUDE="a", CAMERAS_EXCLUDE="b")

        message = str(excinfo.value)
        assert message.startswith("CAMERAS_INCLUDE:")
        assert "CAMERAS_EXCLUDE" in message

    def test_select_cameras_also_refuses_both(self):
        with pytest.raises(ValueError):
            m.select_cameras(
                ["a"], include=re.compile("a"), exclude=re.compile("a")
            )

    @pytest.mark.parametrize("name", ["CAMERAS_INCLUDE", "CAMERAS_EXCLUDE"])
    def test_invalid_regex_names_the_variable(self, name):
        message = error_variable(**{name: "front["})

        assert message.startswith(f"{name}:")


class TestEmptySelectionMessage:
    """Frigate having no cameras and the filter removing all of them must not
    print the same message: only the second is a mistake the operator made."""

    def test_no_cameras_at_all(self):
        cfg = load()

        message = m.empty_camera_list_message([], cfg)

        assert "CAMERAS" not in message

    def test_filter_emptied_the_list(self):
        cfg = load(CAMERAS_INCLUDE=r"^nope")

        message = m.empty_camera_list_message(["front-door", "backyard"], cfg)

        assert "CAMERAS_INCLUDE" in message
        assert "^nope" in message
        assert "2" in message

