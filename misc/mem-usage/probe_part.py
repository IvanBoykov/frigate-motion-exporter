"""Live peak for ONE real upload: the tightest measure of the part-copy change.

Calls the real ``_upload_clip_hybrid_sync`` with a real boto3 client against the
fake S3, once per process, and reports the tracemalloc peak over the call. One
upload has no allocator history to hide behind, so the difference between two
revisions of the uploader is the difference in what the uploader itself holds.

usage: probe_part.py [clip_mib] [part_mib] [chunk_kib]
"""
import gc
import os
import sys
import tracemalloc

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, HERE)

from fake_services import FrigateHandler, S3Handler, STORE, serve  # noqa: E402

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import frigate_s3_archiver as arch  # noqa: E402


def main():
    clip = int(float(sys.argv[1]) * 1024 * 1024) if len(sys.argv) > 1 else 10 << 20
    part = int(float(sys.argv[2]) * 1024 * 1024) if len(sys.argv) > 2 else 5 << 20
    chunk = int(float(sys.argv[3]) * 1024) if len(sys.argv) > 3 else 1 << 20

    s3srv = serve(S3Handler)
    fsrv = serve(FrigateHandler)
    FrigateHandler.clip_bytes = clip
    os.environ["AWS_ENDPOINT_URL_S3"] = (
        f"http://127.0.0.1:{s3srv.server_address[1]}")
    frigate = arch.make_frigate_client(
        f"http://127.0.0.1:{fsrv.server_address[1]}", "", "")
    s3 = arch.make_s3_client()

    # warm the code path so imports/caches are not charged to the peak
    arch._upload_clip_hybrid_sync(
        camera="warm", start_ts=1_700_000_000.0,
        end_ts=1_700_000_060.0, frigate_client=frigate, s3_bucket="clips",
        s3_client=s3, key_timezone=arch.DEFAULT_S3_KEY_TIMEZONE,
        part_size=part, http_chunk_size=chunk, http_timeout=60)

    gc.collect()
    tracemalloc.start()
    base, _ = tracemalloc.get_traced_memory()
    arch._upload_clip_hybrid_sync(
        camera="cam", start_ts=1_800_000_000.0,
        end_ts=1_800_000_060.0, frigate_client=frigate, s3_bucket="clips",
        s3_client=s3, key_timezone=arch.DEFAULT_S3_KEY_TIMEZONE,
        part_size=part, http_chunk_size=chunk, http_timeout=60)
    cur, peak = tracemalloc.get_traced_memory()
    snap = tracemalloc.take_snapshot()
    tracemalloc.stop()

    print(f"clip={clip >> 20}MiB part={part >> 20}MiB chunk={chunk >> 10}KiB")
    print(f"live peak above baseline: {(peak - base) / 1048576:.2f} MiB")
    print(f"live at end (should be ~0): {(cur - base) / 1048576:.2f} MiB")
    print("largest surviving allocations:")
    for st in snap.statistics("lineno")[:6]:
        print(f"   {st}")
    with STORE.lock:
        sizes = list(STORE.part_sizes.values())
    print(f"part sizes on the wire: {sizes}")
    s3srv.shutdown()
    fsrv.shutdown()


if __name__ == "__main__":
    main()
