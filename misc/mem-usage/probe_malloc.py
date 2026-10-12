"""Is the RSS rise live Python buffers, or allocator retention?

Replays the uploader's exact allocation pattern (bytearray extend in 1 MiB
chunks, grow to part_size, bytes(buffer[:part]) copy, del buffer[:part],
256 KiB tail ring) WITHOUT any HTTP, in N threads, and measures RSS:

  * live Python bytes at peak (tracemalloc)
  * RSS peak and RSS after the series ends (the ratchet)
  * RSS after malloc_trim(0)  -- glibc gives back what it can

Run:
    python3 research/mem/probe_malloc.py
    MALLOC_ARENA_MAX=1 python3 research/mem/probe_malloc.py
    MALLOC_TRIM_THRESHOLD_=-1 MALLOC_MMAP_THRESHOLD_=131072 \\
        python3 research/mem/probe_malloc.py
"""
import ctypes
import ctypes.util
import os
import sys
import threading
import time
import tracemalloc

CHUNK = 1 * 1024 * 1024
PART = 5 * 1024 * 1024
TAIL = 512 * 1024
CLIP = 10 * 1024 * 1024


def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("no VmRSS")


def libc_trim():
    lib = ctypes.CDLL(ctypes.util.find_library("c"))
    return int(lib.malloc_trim(0))


def one_upload():
    """The uploader's buffer choreography, byte for byte."""
    buffer = bytearray()
    tail = bytearray()
    parts = []
    remaining = CLIP
    while remaining > 0:
        piece = min(CHUNK, remaining)
        chunk = b"\0" * piece
        buffer.extend(chunk)
        tail.extend(chunk)
        if len(tail) > TAIL:
            del tail[:len(tail) - TAIL]
        remaining -= piece
        while len(buffer) >= PART:
            part = bytes(buffer[:PART])
            del buffer[:PART]
            parts.append(part)
            # upload_part() would be here; the part is dead right after it
            parts.pop()
    if buffer:
        parts.append(bytes(buffer))
        parts.pop()
    return len(tail)


def series(n, threads):
    """Run n uploads, `threads` at a time; return (peak_live, rss_peak, rss_end)."""
    live_peak = [0]
    rss_peak = [0]
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            rss_peak[0] = max(rss_peak[0], rss_kb())
            time.sleep(0.002)

    def watch_live():
        while not stop.is_set():
            cur, _ = tracemalloc.get_traced_memory()
            live_peak[0] = max(live_peak[0], cur)
            time.sleep(0.002)

    base = rss_kb()
    t = threading.Thread(target=watch, daemon=True)
    t.start()
    tracemalloc.start()
    tl = threading.Thread(target=watch_live, daemon=True)
    tl.start()

    def worker():
        for _ in range(n // threads):
            one_upload()

    ws = [threading.Thread(target=worker) for _ in range(threads)]
    for w in ws:
        w.start()
    for w in ws:
        w.join()
    stop.set()
    t.join(timeout=1)
    tl.join(timeout=1)
    _, py_now = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    end = rss_kb()
    print(f"  threads={threads} uploads={n}: live_python_peak="
          f"{live_peak[0]/1048576:5.1f}MiB  rss_base={base/1024:6.1f} "
          f"rss_peak={rss_peak[0]/1024:6.1f} (+{(rss_peak[0]-base)/1024:5.1f}) "
          f"rss_after={end/1024:6.1f} (+{(end-base)/1024:5.1f})")
    return base, end


def main():
    print(f"env: MALLOC_ARENA_MAX={os.environ.get('MALLOC_ARENA_MAX', '(unset)')} "
          f"MALLOC_TRIM_THRESHOLD_={os.environ.get('MALLOC_TRIM_THRESHOLD_', '(unset)')} "
          f"MALLOC_MMAP_THRESHOLD_={os.environ.get('MALLOC_MMAP_THRESHOLD_', '(unset)')} "
          f"cpus={os.cpu_count()}")
    print(f"pattern: {CLIP//1048576}MiB clip, {CHUNK//1048576}MiB chunk, "
          f"{PART//1048576}MiB part, {TAIL//1024}KiB tail ring")
    print("per-camera live math: part(5)+tail(0.5)+chunk(1)+bytes-copy(5) "
          "= 11.5 MiB")

    for threads in (1, 2, 4, 8):
        series(24, threads)

    base, end = series(24, 2)
    freed = libc_trim()
    after = rss_kb()
    print(f"malloc_trim(0) returned {freed} pages -> RSS "
          f"{end/1024:.1f} -> {after/1024:.1f} MiB "
          f"(ratchet was {(end-base)/1024:.1f}, now {(after-base)/1024:.1f})")


if __name__ == "__main__":
    main()
