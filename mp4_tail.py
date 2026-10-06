"""Completeness check for an MP4, using only the stream's tail.

A file that ends at EOF with an intact box - whatever its kind - has not been
cut off: a truncated stream ends inside some box, and the box headers around
EOF cannot both start before EOF and end at EOF. So the check reads only box
headers: it searches backwards from EOF for a ``mfra``/``moov``/``mdat``
signature, reads that box's declared size, and asks whether it lands exactly
on EOF. A signature whose size disagrees is a random byte match inside media
data (or a box whose payload was cut), and the search continues to the next
candidate further back. Only after exhausting the whole buffer is the stream
called truncated.

Frigate exports with ffmpeg ``-movflags frag_keyframe+empty_moov``: fragmented
MP4 whose final box is the ``mfra`` index ffmpeg writes only after the last
packet, so a killed export ends mid-fragment and no box's size reaches EOF.
The same rule covers a plain progressive MP4 (ends in ``moov``, or in ``mdat``
with faststart) and any other fragmented MP4 ending in ``mfra``/``mdat``.

A box size of 0 means "extends to end of file" per ISO 14496-12. That is not
accepted as an EOF match: it is trivially satisfied at every zero byte in the
media data, so it would call nearly every truncated stream complete. A
legitimate trailing size-0 box is rare (it exists to avoid back-patching) and
losing it is over-flagging - a retry, then a ``-truncated.mp4`` key - while
under-flagging archives corrupt files as complete.

Two accepted limits follow from box geometry, both measured against real
transcodes:

- a fragmented stream cut exactly at the end of its final ``mdat`` (with only
  ``mfra`` missing) has an intact box at EOF and reads complete;
- a complete stream reads truncated if the given buffer does not reach the
  header of its final box - a progressive faststart MP4 whose last ``mdat``
  spans more than ``TAIL_WINDOW_BYTES`` is the example. Frigate's exports end
  in a small ``mfra``, so the uploader's window always covers the header.

No box contents are parsed: box start plus size fully answers "is this box
intact and is it the last thing in the file". Track durations were never
checked either - with ``-c copy`` ffmpeg carries input durations over, so a
short export can advertise a long duration and a duration comparison produces
false truncations.
"""
import struct

TAIL_SIGNATURES = (b"mfra", b"moov", b"mdat")

# A 10-minute clip is ~1000 fragments; the final box and any candidate
# signatures live within the last chunks. The constant only bounds how far
# the uploader keeps for the search.
TAIL_WINDOW_BYTES = 256 * 1024


def _last_signature(buf, end):
    """Index of the last signature occurrence ending before `end`, or -1."""
    best = -1
    for signature in TAIL_SIGNATURES:
        found = buf.rfind(signature, 0, end)
        if found > best:
            best = found
    return best


def _box_size(buf, box_start, eof):
    """Declared size of the box at box_start, or None if unreadable.

    A size of 1 means a 64-bit size follows the type; a size of 0 or under 8
    is not a usable length and reads as None.
    """
    if box_start + 8 > eof:
        return None
    size = struct.unpack(">I", buf[box_start:box_start + 4])[0]
    if size == 1:
        if box_start + 16 > eof:
            return None
        size = struct.unpack(">Q", buf[box_start + 8:box_start + 16])[0]
    if size < 8:
        return None
    return size


def stream_is_complete(tail):
    """Is ``tail`` (the final bytes of an MP4 stream) the end of a complete file?

    Returns ``(complete, reason)``. Searches backwards for a box whose
    declared size ends exactly at end-of-buffer; such a box proves nothing
    after it was cut away. Returns ``(False, "stream_too_short")`` below the
    smallest parseable header and ``(False, "no_complete_box_at_eof")`` when
    no signature through the start of the buffer yields a box ending at EOF.
    """
    eof = len(tail)
    if eof < 16:
        return False, "stream_too_short"

    search_end = eof
    while True:
        signature = _last_signature(tail, search_end)
        # A box needs its 4-byte size field inside the buffer in front of the
        # signature, so positions 0..3 are dead ends - and so is -1, the
        # "no signature left" marker, which is what ends the search.
        if signature < 4:
            return False, "no_complete_box_at_eof"
        box_start = signature - 4
        size = _box_size(tail, box_start, eof)
        if size is not None and box_start + size == eof:
            return True, "ok"
        # Random match or a box cut short: keep looking further back. Any
        # earlier occurrence starts at least 4 bytes before this one, so the
        # search always progresses towards the start of the buffer.
        search_end = signature
