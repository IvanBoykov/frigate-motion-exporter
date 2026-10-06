"""The last-box-at-EOF detector that guards against truncated clip exports."""
import os
import struct
import subprocess

import pytest

import mp4_tail

FFMPEG = os.environ.get("MT_FFMPEG", "ffmpeg")
has_ffmpeg = os.environ.get("MT_NO_FFMPEG") != "1" and (
    subprocess.run(["which", FFMPEG], capture_output=True).returncode == 0
)

WINDOW = mp4_tail.TAIL_WINDOW_BYTES


def box(typ, payload=b"", sixtyfour=False):
    if sixtyfour:
        return struct.pack(">I4sQ", 1, typ, 16 + len(payload)) + payload
    return struct.pack(">I4s", 8 + len(payload), typ) + payload


def mfra():
    """The trailer FFmpeg writes at EOF, matching the real 0.17.0 byte layout."""
    tfra = struct.pack(">I4s4sIII", 24, b"tfra", b"\x01\x00\x00\x00", 1, 0, 0)
    mfro = struct.pack(">I4s4sI", 16, b"mfro", b"\x00\x00\x00\x00", 8 + len(tfra))
    return box(b"mfra", tfra + mfro)


# Synthetic layouts: no signature bytes anywhere but the real box headers, so
# every verdict below is exact rather than statistical.
FRIGATE_STYLE = (
    box(b"ftyp", b"isom\x00\x00\x02\x00isom")
    + box(b"moov", b"\x00" * 64)
    + box(b"moof", b"\x00" * 40)
    + box(b"mdat", b"y" * 600)
    + mfra()
)
FMP4_ENDS_IN_MDAT = (
    box(b"ftyp", b"isom\x00\x00\x02\x00isom")
    + box(b"moov", b"\x00" * 64)
    + box(b"moof", b"\x00" * 40)
    + box(b"mdat", b"y" * 600)
)
PROGRESSIVE_ENDS_IN_MOOV = box(b"ftyp", b"isom\x00\x00\x02\x00isom") + box(
    b"mdat", b"y" * 900
) + box(b"moov", b"\x00" * 80)
FASTSTART_ENDS_IN_MDAT = box(b"ftyp", b"isom\x00\x00\x02\x00isom") + box(
    b"moov", b"\x00" * 80
) + box(b"mdat", b"y" * 900)

ALL_LAYOUTS = [FRIGATE_STYLE, FMP4_ENDS_IN_MDAT, PROGRESSIVE_ENDS_IN_MOOV,
               FASTSTART_ENDS_IN_MDAT]


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
def test_complete_layout_is_accepted(layout):
    assert mp4_tail.stream_is_complete(layout) == (True, "ok")


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
def test_cut_inside_the_last_box_is_truncated(layout):
    """The regression the feature exists for: a stream cut anywhere inside the
    final box - even one byte before EOF - must never look complete."""
    boundary = last_box(layout)
    for cut in range(boundary + 1, len(layout)):
        complete, _ = mp4_tail.stream_is_complete(layout[:cut])
        assert complete is False, f"cut at {cut} looked complete"


def test_cut_exactly_at_a_box_boundary_reads_complete():
    """A stream cut where one box ends and the next would begin delivered only
    whole boxes: nothing was chopped, so the check says complete even though
    the following box is lost. Accepted, documented behavior."""
    boundary = last_box(FRIGATE_STYLE)  # mfra lost, mdat ends at EOF
    assert mp4_tail.stream_is_complete(FRIGATE_STYLE[:boundary]) == (True, "ok")


def test_cut_ending_on_a_moof_is_truncated():
    """A moof announces an mdat that must follow it; a stream ending with the
    moof lost its media. moof is not an accepted final box, so the check -
    which only knows mfra/moov/mdat - reports truncated."""
    boundary = last_box(FMP4_ENDS_IN_MDAT)  # moof ends, mdat never began
    assert mp4_tail.stream_is_complete(FMP4_ENDS_IN_MDAT[:boundary]) == (
        False,
        "no_complete_box_at_eof",
    )


def test_sixtyfourbit_box_at_eof_is_complete():
    body = FRIGATE_STYLE[: -len(mfra())] + box(b"mdat", b"y" * 100, sixtyfour=True)
    assert mp4_tail.stream_is_complete(body) == (True, "ok")


def test_random_signature_in_media_data_is_skipped():
    """A byte pattern inside the final box that reads like a header whose size
    disagrees with EOF must not hide the real box: the search keeps walking
    back until the size lands on EOF."""
    decoy = struct.pack(">I", 1) + b"moov"  # 64-bit extension over filler bytes
    payload = b"z" * 50 + decoy + b"z" * 40
    body = box(b"ftyp", b"isom\x00\x00\x02\x00isom") + box(b"moov", b"\x00" * 64) + box(b"mdat", payload)
    assert body.rfind(b"moov") > body.rfind(b"mdat")  # the decoy sits after the real header
    assert mp4_tail.stream_is_complete(body) == (True, "ok")


def test_zero_size_box_at_eof_is_not_accepted():
    """Size 0 means 'to end of file' in ISO 14496-12, but that is satisfied by
    any random zero byte in media data, so it never proves completeness."""
    body = (
        FRIGATE_STYLE[: -len(mfra())]
        + struct.pack(">I", 0)
        + b"mdat"
        + b"z" * 16
    )
    assert mp4_tail.stream_is_complete(body)[0] is False


def test_no_signature_anywhere_is_truncated():
    assert mp4_tail.stream_is_complete(b"y" * 100_000) == (
        False,
        "no_complete_box_at_eof",
    )


@pytest.mark.parametrize("prefix_len", [0, 1, 2, 3])
def test_signature_without_room_for_a_size_field_is_truncated(prefix_len):
    """A signature at offset 0..3 has no 4 size bytes in front of it inside
    the buffer, and no signature at all ends the search: both must return,
    never index before the start of the buffer."""
    buf = b"z" * prefix_len + b"mdat" + b"z" * 100
    assert mp4_tail.stream_is_complete(buf) == (False, "no_complete_box_at_eof")


@pytest.mark.parametrize("size", [0, 8, 15])
def test_smaller_than_a_header_is_truncated(size):
    assert mp4_tail.stream_is_complete(b"z" * size) == (False, "stream_too_short")


def test_final_box_header_outside_the_buffer_is_truncated():
    """The accepted window limit: the final mdat reaches EOF intact, but the
    buffer does not reach its header, so completeness cannot be proven and the
    safe verdict is truncated."""
    body = (
        box(b"ftyp", b"isom\x00\x00\x02\x00isom")
        + box(b"moov", b"\x00" * 64)
        + box(b"mdat", b"y" * 100_000)
    )
    assert mp4_tail.stream_is_complete(body[-64:]) == (
        False,
        "no_complete_box_at_eof",
    )


def last_box(data):
    """Offset of the final top-level box, walking sizes from the head."""
    pos = 0
    last = 0
    while pos + 8 <= len(data):
        size = struct.unpack(">I", data[pos:pos + 4])[0]
        if size == 1:
            size = struct.unpack(">Q", data[pos + 8:pos + 16])[0]
        assert size >= 8 and pos + size <= len(data), "fixture must be walkable"
        last = pos
        pos += size
    assert pos == len(data)
    return last


def last_box_size(data):
    pos = last_box(data)
    size = struct.unpack(">I", data[pos:pos + 4])[0]
    if size == 1:
        size = struct.unpack(">Q", data[pos + 8:pos + 16])[0]
    return size


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    if not has_ffmpeg:
        pytest.skip("ffmpeg unavailable")
    src = tmp_path_factory.mktemp("mt") / "src.mp4"
    subprocess.run(
        [
            FFMPEG, "-v", "error", "-y",
            "-f", "lavfi", "-i", "testsrc2=duration=8:size=320x240:rate=15",
            "-c:v", "libx264", "-preset", "ultrafast", "-g", "15",
            str(src),
        ],
        check=True,
    )
    return src


def transcode(source, *flags):
    """Mux to a file: progressive mp4 cannot be written to a pipe (the muxer
    seeks back to patch sizes), and the file is what the check must classify."""
    out = source.parent / "out.mp4"
    subprocess.run(
        [FFMPEG, "-v", "error", "-y", "-i", str(source), "-c", "copy",
         *flags, str(out)],
        check=True,
    )
    return out.read_bytes()


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_frigate_export_layout_is_complete(source):
    """The layout the deployed Frigate produces: ftyp + empty moov +
    (moof mdat)* + mfra."""
    data = transcode(source, "-movflags", "frag_keyframe+empty_moov")
    assert mp4_tail.stream_is_complete(data[-WINDOW:]) == (True, "ok")
    assert mp4_tail.stream_is_complete(data) == (True, "ok")


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_progressive_mp4_is_complete(source):
    """Default muxer: the (big) mdat first, the moov at EOF."""
    data = transcode(source)
    assert mp4_tail.stream_is_complete(data[-WINDOW:]) == (True, "ok")


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_fragmented_with_nonempty_moov_is_complete(source):
    """frag_keyframe without empty_moov: a filled moov up front, still an mfra
    at EOF - the other fragmented variant ffmpeg emits."""
    data = transcode(source, "-movflags", "frag_keyframe")
    assert mp4_tail.stream_is_complete(data[-WINDOW:]) == (True, "ok")


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_file_killed_inside_the_trailer_is_detected(source):
    data = transcode(source, "-movflags", "frag_keyframe+empty_moov")
    mfra_start = len(data) - last_box_size(data)
    assert data[last_box(data) + 4:last_box(data) + 8] == b"mfra"
    for cut in (mfra_start + 1, mfra_start + 10, len(data) - 2):
        assert mp4_tail.stream_is_complete(data[:cut][-WINDOW:]) == (
            False,
            "no_complete_box_at_eof",
        )


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_file_cut_deep_inside_a_fragment_is_detected(source):
    """A cut hundreds of kilobytes from EOF - where a killed ffmpeg actually
    dies - must be detected, not just cuts at the very end."""
    data = transcode(source, "-movflags", "frag_keyframe+empty_moov")
    mdat_start = data.rfind(b"mdat") - 4  # the file ends in mfra + mdat last
    assert mdat_start > len(data) - WINDOW  # its header is inside the window
    complete, _ = mp4_tail.stream_is_complete(data[: mdat_start + 50][-WINDOW:])
    assert complete is False


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_progressive_cut_inside_the_moov_is_detected(source):
    data = transcode(source)
    assert mp4_tail.stream_is_complete(data[:-4][-WINDOW:]) == (
        False,
        "no_complete_box_at_eof",
    )


@pytest.mark.skipif(not has_ffmpeg, reason="ffmpeg unavailable")
def test_real_faststart_mdat_reaching_past_the_window_is_flagged(source):
    """The other face of the window limit, on a real file: faststart moves the
    moov to the head and the trailing mdat is bigger than the window, so no
    visible header can prove the final bytes intact - a complete file is
    conservatively flagged. Frigate exports end in a small mfra instead, which
    is why the uploader window always suffices for them."""
    data = transcode(source, "-movflags", "+faststart")
    assert len(data) > WINDOW  # the trailing mdat cannot fit the window
    assert mp4_tail.stream_is_complete(data[-WINDOW:]) == (
        False,
        "no_complete_box_at_eof",
    )
    # The whole file - header included - is accepted.
    assert mp4_tail.stream_is_complete(data) == (True, "ok")
