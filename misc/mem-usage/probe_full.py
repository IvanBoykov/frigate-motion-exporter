"""RSS of the WHOLE pipeline, fakes out-of-process, 1 ms sampling.

Same fake services as fake_services.py, but the measurement is the production
path: config.load_config() -> _run_cameras() -> supervise_camera -> run_camera
-> find_motion_intervals -> upload_clip_hybrid, with the metrics HTTP server up.
That is the code that reports process_resident_memory_bytes, so the number
measured here is the number the alert sees.

The fakes run in a CHILD process by default. Two reasons, both measured, both
fatal to the old numbers:

  * RSS. An in-process fake shares /proc/self with the archiver, so payload
    generation, body hashing and per-connection buffers are charged to the
    uploader - the same error the streaming-digest fix removed one layer
    deeper.
  * Allocator environment. MALLOC_* variables are per-process. Set on an
    in-process fake they also re-price the fake's own work (256 KiB payload
    chunks, 64 KiB read blocks), so a claim like "MALLOC_TRIM_THRESHOLD_
    costs 2.6x CPU" partly measures the FAKE being slowed down, not the
    uploader. Out-of-process, the environment moves only the uploader.

usage: probe_full.py [clip_mib] [seconds] [speed] [cameras] [throttle_ms]
                     [segments_per_hour] [clip_target] [--snap] [--inproc]

speed = accelerated wall seconds per real second (60 = hard catch-up).
cameras = how many cameras the fake Frigate advertises and the archiver runs.
throttle_ms = pause the fake S3 inserts per 64 KiB read, to apply backpressure
and hold uploads in flight together.
segments_per_hour sizes the hour of footage the fake reports (segments are
spaced beyond max_gap, so each segment becomes one clip); clip_target ends the
run once that many clips have been stored. Together they give every
configuration of a benchmark series the identical upload volume, which is what
makes the CPU totals comparable.
--snap turns on tracemalloc; tracemalloc itself allocates, so the live-bytes
mode and the clean-RSS mode are separate runs by design.
--inproc runs the fakes inside the measured process (old behaviour; kept for
A/B-ing the harness itself, never for reporting numbers).
"""
import asyncio
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, HERE)

from fake_services import (  # noqa: E402
    FrigateHandler, S3Handler, admin, serve, spawn, stop)

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import config as config_mod  # noqa: E402
import frigate_s3_archiver as arch  # noqa: E402


def rss_kb(pid=None):
    with open(f"/proc/{pid or 'self'}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("no VmRSS")


def fake_usage(s3_port, inproc):
    """(cpu_seconds, user, sys, minflt, rss_kib) of the fake-services process.

    measure.py takes CPU from RUSAGE_CHILDREN, which folds in every waited-for
    descendant - the out-of-process fakes included. Their work is a constant
    per volume, but not a constant per configuration: an allocator setting
    reprices the fake's hashing and chunking too, so the uploader's own CPU
    and faults must be reported with the fake's subtracted. The fake's numbers
    are printed too, never silently dropped.

    The usage comes from the fake's /__admin, not /proc: this sandbox hides
    /proc/<pid> of other processes, so only the child itself can report about
    itself (it shares one process with both fakes, so one report covers both).
    """
    if inproc:
        return 0.0, 0.0, 0.0, 0, 0
    usage = admin(s3_port)["usage"]
    return (usage["utime"] + usage["stime"], usage["utime"], usage["stime"],
            usage["minflt"], usage["rss_kib"])


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    clip_mb = float(args[0]) if len(args) > 0 else 10.0
    seconds = float(args[1]) if len(args) > 1 else 25.0
    speed = float(args[2]) if len(args) > 2 else 60.0
    cameras = int(args[3]) if len(args) > 3 else 2
    throttle_ms = float(args[4]) if len(args) > 4 else 0.0
    segments = int(args[5]) if len(args) > 5 else 60
    clip_target = int(args[6]) if len(args) > 6 else 0
    snap_mode = "--snap" in sys.argv
    inproc = "--inproc" in sys.argv

    fake_proc = None
    s3_port = f_port = 0
    if inproc:
        s3srv = serve(S3Handler)
        fsrv = serve(FrigateHandler)
        s3_port = s3srv.server_address[1]
        f_port = fsrv.server_address[1]
        FrigateHandler.clip_bytes = int(clip_mb * 1024 * 1024)
        FrigateHandler.segments_per_hour = segments
        FrigateHandler.n_cameras = cameras
        S3Handler.throttle_sleep = throttle_ms / 1000.0
    else:
        fake_proc, s3_port, f_port = spawn(
            int(clip_mb * 1024 * 1024), segments, cameras, throttle_ms)

    try:
        run_probe(clip_mb, seconds, speed, cameras, throttle_ms,
                  clip_target, snap_mode, inproc, s3_port, f_port, fake_proc,
                  (s3srv, fsrv) if inproc else None)
    finally:
        if fake_proc is not None:
            stop(fake_proc)


def run_probe(clip_mb, seconds, speed, cameras, throttle_ms, clip_target,
              snap_mode, inproc, s3_port, f_port, fake_proc, inproc_servers):
    os.environ["AWS_ENDPOINT_URL_S3"] = f"http://127.0.0.1:{s3_port}"
    cam_list = "|".join(f"cam{i}" for i in range(cameras))

    env = {
        "S3_BUCKET": "clips",
        "FRIGATE_URL": f"http://127.0.0.1:{f_port}",
        "CAMERAS_INCLUDE": cam_list,
        "IDLE_SLEEP_SECONDS": "1.0",
        "LOG_LEVEL": "WARNING",
    }
    cfg = config_mod.load_config(env)
    print(f"clip={clip_mb}MiB cams={cameras} speed={speed}x "
          f"throttle={throttle_ms}ms snap={snap_mode} inproc={inproc} "
          f"MALLOC_ARENA_MAX={os.environ.get('MALLOC_ARENA_MAX', '(unset)')}")

    from prometheus_client import start_http_server
    start_http_server(0)

    import tracemalloc
    print(f"RSS before run: {rss_kb()/1024:.1f} MiB")
    samples = []
    fake_samples = []
    live = []
    stop_flag = threading.Event()
    if snap_mode:
        tracemalloc.start()

    def sampler():
        while not stop_flag.is_set():
            samples.append((time.perf_counter(), rss_kb()))
            time.sleep(0.001)

    def fake_sampler():
        # The fake's RSS is nice-to-know, polled slowly: every poll is an
        # HTTP request whose cost lands on the measured process, so it must
        # not run at the 1 ms cadence of the real sampler.
        while not stop_flag.is_set():
            try:
                fake_samples.append(admin(s3_port)["usage"]["rss_kib"])
            except Exception:
                pass
            time.sleep(0.1)

    peak_holder = {"peak": 0, "snap": None}

    def live_watcher():
        while not stop_flag.is_set():
            cur, _ = tracemalloc.get_traced_memory()
            if cur > peak_holder["peak"]:
                peak_holder["peak"] = cur
                peak_holder["snap"] = tracemalloc.take_snapshot()
            live.append((time.perf_counter(), cur))
            time.sleep(0.05)

    threading.Thread(target=sampler, daemon=True).start()
    if not inproc:
        threading.Thread(target=fake_sampler, daemon=True).start()
    if snap_mode:
        threading.Thread(target=live_watcher, daemon=True).start()

    t0 = time.time()
    fake_cpu0, _, fake_sys0, fake_minflt0, _ = fake_usage(s3_port, inproc)

    def now_fn():
        return t0 + (time.time() - t0) * speed

    def objects_done():
        if inproc:
            from fake_services import STORE
            with STORE.lock:
                return len(STORE.objects)
        return admin(s3_port)["objects"]

    async def run():
        task = asyncio.ensure_future(
            arch._run_cameras(cfg, "clips", cfg.s3_key_timezone, now_fn)
        )
        # A clip target ends every configuration of a series with the identical
        # upload volume, which is what makes the CPU totals comparable; seconds
        # stays as the safety cap.
        deadline = time.perf_counter() + seconds
        while time.perf_counter() < deadline:
            if clip_target and objects_done() >= clip_target:
                break
            await asyncio.sleep(0.25)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    stop_flag.set()
    time.sleep(0.05)

    peak = max(v for _, v in samples)
    final = samples[-1][1]
    print(f"RSS peak during run : {peak/1024:.1f} MiB")
    print(f"RSS at end of run   : {final/1024:.1f} MiB")
    if snap_mode and live:
        peak_live, snap = peak_holder["peak"], peak_holder["snap"]
        tracemalloc.stop()
        print(f"live Python bytes at peak: {peak_live/1048576:.1f} MiB")
        if snap is not None:
            print("top allocation sites at the live peak:")
            for st in snap.statistics("lineno")[:12]:
                print(f"   {st}")
    import ctypes
    import ctypes.util
    ctypes.CDLL(ctypes.util.find_library("c")).malloc_trim(0)
    print(f"RSS after malloc_trim(0): {rss_kb()/1024:.1f} MiB")
    if fake_samples:
        print(f"fake RSS peak       : {max(fake_samples)/1024:.1f} MiB")
    fake_cpu1, _, fake_sys1, fake_minflt1, _ = fake_usage(s3_port, inproc)
    print(f"fake cpu excluded   : {fake_cpu1 - fake_cpu0:.1f}")
    print(f"fake sys excluded   : {fake_sys1 - fake_sys0:.1f}")
    print(f"fake minflt excluded: {max(fake_minflt1 - fake_minflt0, 0)}")

    status = admin(s3_port, full=True)
    n_obj = status["objects"]
    mismatched = status.get("mismatched", -1)
    print(f"objects written     : {n_obj}")
    print(f"clips per camera    : {status.get('clips_per_camera')}")
    print(f"peak concurrent uploads : {status.get('peak_concurrent')}")
    print(f"byte-identity check : "
          f"{'OK - every object matches the clip stream' if mismatched == 0 else f'FAIL for {mismatched} objects'}")
    patterns = sorted({tuple(v) for v in status.get("part_sizes", {}).values()})
    print(f"part-size patterns  : {patterns}")
    if samples:
        t_start = samples[0][0]
        rel = [(t - t_start, v) for t, v in samples]
        buckets = []
        for b in range(0, int(seconds), 2):
            win = [v for t, v in rel if b <= t < b + 2]
            if win:
                buckets.append(f"{max(win)/1024:.0f}")
        print("RSS peak per 2 s bucket (MiB):", " ".join(buckets))
    if inproc_servers:
        inproc_servers[0].shutdown()
        inproc_servers[1].shutdown()


if __name__ == "__main__":
    main()
