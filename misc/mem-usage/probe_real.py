"""The real exporter pipeline against the real Frigate, writing to fake S3.

Frigate is the production one (``FRIGATE_URL`` plus basic auth from the
environment, never printed); S3 is the loopback fake, so nothing leaves this
machine. The point is the byte check:
the object the fake S3 accepted must hash to exactly what a plain
requests.get of the same export returns, over a real MP4 at a real bitrate.

usage: FRIGATE_URL=https://... probe_real.py <camera> [minutes]
"""
import base64
import hashlib
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, HERE)

from fake_services import S3Handler, STORE, serve  # noqa: E402

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import frigate_s3_archiver as arch  # noqa: E402


def rss_mib():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    raise RuntimeError("no VmRSS")


def main():
    camera = sys.argv[1] if len(sys.argv) > 1 else "dacha"
    minutes = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    user = os.environ["FRIGATE_USER"]
    password = os.environ["FRIGATE_PASSWORD"]
    base = os.environ["FRIGATE_URL"]

    s3srv = serve(S3Handler)
    os.environ["AWS_ENDPOINT_URL_S3"] = (
        f"http://127.0.0.1:{s3srv.server_address[1]}")
    frigate = arch.make_frigate_client(base, user, password)
    s3 = arch.make_s3_client()

    end = int(time.time()) - 120
    start = end - minutes * 60

    # the reference: the same export, fetched the boring way
    import requests
    tok = base64.b64encode(f"{user}:{password}".encode()).decode()
    ref = requests.get(
        f"{base}/api/{camera}/start/{start}/end/{end}/clip.mp4",
        headers={"Authorization": f"Basic {tok}"}, stream=True, timeout=600)
    ref.raise_for_status()
    ref_digest = hashlib.sha256()
    ref_len = 0
    for chunk in ref.iter_content(1 << 16):
        ref_digest.update(chunk)
        ref_len += len(chunk)

    base_rss = rss_mib()
    peak = base_rss
    t0 = time.perf_counter()
    arch._upload_clip_hybrid_sync(
        camera=camera, start_ts=float(start), end_ts=float(end),
        frigate_client=frigate, s3_bucket="clips", s3_client=s3,
        key_timezone=arch.DEFAULT_S3_KEY_TIMEZONE,
        part_size=arch.DEFAULT_PART_SIZE,
        http_chunk_size=arch.DEFAULT_HTTP_CHUNK_SIZE, http_timeout=600)
    dt = time.perf_counter() - t0
    peak = max(peak, rss_mib())

    with STORE.lock:
        objects = dict(STORE.objects)
        parts = dict(STORE.part_sizes)
    got = [(k, v) for k, v in objects.items() if camera in k]
    print(f"{camera}: {minutes} min export, {ref_len / 1e6:.1f} MB "
          f"in {dt:.1f} s")
    print(f"RSS: base {base_rss:.1f} MiB, after upload {peak:.1f} MiB")
    for k, (size, digest) in got:
        print(f"  stored {k}: {size} B")
        print(f"  byte-identical to a plain requests.get: "
              f"{digest == ref_digest.hexdigest() and size == ref_len}")
        print(f"  part sizes: {parts.get(k)}")
    if not got:
        print("  no object stored - the exporter chose a different key; "
              f"stored keys: {list(objects)[:3]}")
    s3srv.shutdown()


if __name__ == "__main__":
    main()
