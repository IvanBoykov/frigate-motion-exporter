"""Fake Frigate + fake S3 over real loopback HTTP.

No mock of the code under test: the archiver gets a real requests.Session and
a real boto3 client, so urllib3/botocore buffering is part of the measurement.

Run as a program (`python3 fake_services.py <clip_bytes> <segments> <cams>
[throttle_ms]`) it serves both fakes out-of-process and prints `PORTS <s3>
<frigate>` on stdout; probes use `spawn()` for that. The measurement reads
RSS of the archiver process only, so the fakes' allocation churn (payload
generation, body hashing, socket buffers) must not share that process: an
in-process fake taxes the same allocator the probe is measuring and inflates
the per-camera slope by roughly its own working set. An in-process `GET
/__admin` endpoint on the fake S3 reports object count, and with `?full=1`
the whole integrity picture, so the parent can verify a run without ever
holding a body.

`serve()` keeps the in-process mode for probe_real.py, whose subject is the
byte check against a real Frigate, not RSS.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from xml.etree import ElementTree as ET

BUCKET = "clips"


class Store:
    """Server-side state; bodies are dropped, only size and digest kept.

    Retaining uploaded bytes here would charge them to the measured RSS and
    invent an uploader cost that real S3 does not impose on this process. The
    incremental digest is what lets a probe prove the bytes on the wire are
    unchanged, part boundaries included.
    """
    def __init__(self):
        self.objects = {}          # key -> (size, hex digest)
        self.mpu = {}              # upload_id -> {"parts": {no: size}, "h": hash}
        self.part_sizes = {}       # key -> [part size, ...] in upload order
        self.lock = threading.Lock()
        self.counter = 0
        self.requests = []         # (method, path) for assertions

    def put(self, key, body):
        self.objects[key] = (len(body), hashlib.sha256(body).hexdigest())
        self.part_sizes.setdefault(key, []).append(len(body))

    def put_digest(self, key, size, digest):
        """Streaming variant: the fake never holds a whole body, or its
        buffering would be charged to the uploader's RSS."""
        self.objects[key] = (size, digest)
        self.part_sizes.setdefault(key, []).append(size)

    def mpu_part(self, upid, part_no, body):
        self.mpu_part_chunk(upid, part_no, body)

    def mpu_part_chunk(self, upid, part_no, data):
        # one running hash over parts in arrival order: a match proves the
        # bytes reached the wire in the right order, with no gap or overlap.
        # Chunks of one request arrive in order from one thread; parts of one
        # clip are sequential in the archiver, matching the old whole-body API.
        entry = self.mpu.setdefault(
            upid, {"sizes": [], "order": [], "h": hashlib.sha256(), "cur": None}
        )
        if entry["cur"] != part_no:
            entry["order"].append(part_no)
            entry["sizes"].append(0)
            entry["cur"] = part_no
        entry["h"].update(data)
        entry["sizes"][-1] += len(data)

    def mpu_complete(self, key, upid):
        entry = self.mpu.pop(
            upid, {"sizes": [], "order": [], "h": hashlib.sha256()}
        )
        self.objects[key] = (sum(entry["sizes"]), entry["h"].hexdigest())
        self.part_sizes[key] = entry["sizes"]


STORE = Store()


def _xml(root_tag, inner, ns="http://s3.amazonaws.com/doc/2006-03-01/"):
    return (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<{root_tag} xmlns="{ns}">{inner}</{root_tag}>').encode()


class Concurrency:
    """How many clip uploads were in flight at the same instant.

    The claim that resident memory scales with cameras only holds if the
    uploads actually overlap, so this measures the overlap rather than
    assuming it: an upload id is live from its first part until Complete, a
    single-PUT clip for as long as its body is being read.
    """
    live = {}          # upload_id -> camera
    single = 0         # single-PUT bodies being read now
    peak = 0
    clips = {}         # camera -> clips started
    lock = threading.Lock()

    @classmethod
    def part(cls, upid, key):
        cam = key.split("/")[1]
        with cls.lock:
            if upid not in cls.live:
                cls.clips[cam] = cls.clips.get(cam, 0) + 1
            cls.live[upid] = cam
            cls.peak = max(cls.peak, len(cls.live) + cls.single)

    @classmethod
    def complete(cls, upid):
        with cls.lock:
            cls.live.pop(upid, None)

    @classmethod
    def single_enter(cls, key):
        cam = key.split("/")[1]
        with cls.lock:
            cls.single += 1
            cls.clips[cam] = cls.clips.get(cam, 0) + 1
            cls.peak = max(cls.peak, len(cls.live) + cls.single)

    @classmethod
    def single_leave(cls):
        with cls.lock:
            cls.single -= 1


class S3Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # A pause per read chunk applies TCP backpressure to the uploader, which is
    # how the probe holds uploads in flight together so the per-camera working
    # sets coexist. 0 still streams bodies but without throttling.
    throttle_sleep = 0.0
    throttle_chunk = 64 * 1024

    def log_message(self, *args):
        pass

    # -- helpers ---------------------------------------------------------
    def _read_stream(self, n, sink):
        """Read n bytes in chunks, handing each to sink as it arrives, and
        optionally sleep between chunks so the sender's socket buffers fill up.

        The fake S3 shares this process with the measured RSS, so holding a
        whole body here would charge server-side buffering to the uploader and
        invent an S3 cost the real endpoint does not impose on this process.
        """
        size = 0
        while size < n:
            want = min(self.throttle_chunk, n - size)
            block = self.rfile.read(want)
            if not block:
                break
            size += len(block)
            sink(block)
            del block
            if self.throttle_sleep:
                time.sleep(self.throttle_sleep)
        return size

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        data = self.rfile.read(n) if n else b""
        if self.headers.get("Transfer-Encoding") == "chunked":
            out = bytearray()
            while True:
                line = self.rfile.readline().strip()
                if not line:
                    continue
                size = int(line, 16)
                if size == 0:
                    self.rfile.read(2)
                    break
                out += self.rfile.read(size)
                self.rfile.read(2)
            data = bytes(out)
        return data

    def _send(self, code, body=b"", headers=None, ctype="application/xml"):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    # -- verbs -----------------------------------------------------------
    def do_HEAD(self):
        path = urlparse(self.path).path
        key = path.split("/", 1)[1] if "/" in path else ""
        with STORE.lock:
            entry = STORE.objects.get(key)
            size = entry[0] if entry else None
        if size is None:
            self._send(404, _xml("Error", "<Code>NoSuchKey</Code>"))
            return
        self._send(200, b"", {"ETag": '"deadbeef"'})

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query, keep_blank_values=True)
        if u.path == "/__admin":
            self._admin(q)
            return
        with STORE.lock:
            STORE.requests.append(("GET", self.path))
        if "list-type=2" in self.path or "list-type" in q:
            prefix = (q.get("prefix") or [""])[0]
            token = (q.get("continuation-token") or [""])[0]
            with STORE.lock:
                keys = sorted(k for k in STORE.objects if k.startswith(prefix))
            page_size = 1000
            start = int(token) if token else 0
            page = keys[start:start + page_size]
            inner = "".join(
                f"<Contents><Key>{k}</Key><Size>{STORE.objects.get(k, (0, ''))[0]}</Size>"
                f"<ETag>\"x\"</ETag></Contents>" for k in page
            )
            more = start + page_size < len(keys)
            if more:
                inner += f"<NextContinuationToken>{start + page_size}</NextContinuationToken>"
            body = _xml("ListBucketResult",
                        f"<IsTruncated>{str(more).lower()}</IsTruncated>"
                        f"{inner}")
            self._send(200, body)
            return
        key = u.path.split("/", 1)[1] if "/" in u.path else ""
        with STORE.lock:
            entry = STORE.objects.get(key)
            size = entry[0] if entry else None
        if size is None:
            self._send(404, _xml("Error", "<Code>NoSuchKey</Code>"))
            return
        self._send(200, b"", {"ETag": '"deadbeef"'})

    def _admin(self, q):
        """Probe-facing status. The cheap form is polled for progress; the
        full form hashes the expected clip once and verifies every object."""
        with STORE.lock:
            n = len(STORE.objects)
            objects = dict(STORE.objects) if "full" in q else None
        status = {"objects": n, "usage": self_usage()}
        if objects is not None:
            fh = FrigateHandler.__new__(FrigateHandler)
            expected = fh.expected_digest()
            mismatched = sum(1 for _, digest in objects.values()
                             if digest != expected)
            with STORE.lock:
                part_sizes = {k: v for k, v in STORE.part_sizes.items()
                              if k in objects}
            status.update({
                "mismatched": mismatched,
                "part_sizes": part_sizes,
                "peak_concurrent": Concurrency.peak,
                "clips_per_camera": dict(Concurrency.clips),
            })
        self._send(200, json.dumps(status).encode(),
                   ctype="application/json")

    def do_PUT(self):
        u = urlparse(self.path)
        q = parse_qs(u.query, keep_blank_values=True)
        key = u.path.split("/", 1)[1] if "/" in u.path else ""
        is_part = "uploadId" in q and "partNumber" in q
        is_init = "uploads" in q
        n = int(self.headers.get("Content-Length") or 0)
        if is_part:
            upid, part = q["uploadId"][0], int(q["partNumber"][0])
            Concurrency.part(upid, key)
            self._read_stream(n, lambda b: STORE.mpu_part_chunk(upid, part, b))
            self._send(200, b"", {"ETag": f'"p{part}"'})
            return
        if "uploads" not in q and "uploadId" not in q:
            h = hashlib.sha256()
            Concurrency.single_enter(key)
            try:
                size = self._read_stream(n, h.update)
            finally:
                Concurrency.single_leave()
            with STORE.lock:
                STORE.requests.append(("PUT", self.path))
                STORE.put_digest(key, size, h.hexdigest())
            self._send(200, b"", {"ETag": '"put1"'})
            return
        body = self._body()  # initiate (no body) and any chunked remainder
        del body
        with STORE.lock:
            STORE.requests.append(("PUT", self.path))
            STORE.counter += 1
            counter = STORE.counter
            STORE.mpu[f"up{counter}"] = {"sizes": [], "order": [],
                                         "h": hashlib.sha256(), "cur": None}
        self._send(200, _xml("InitiateMultipartUploadResult",
                             f"<Key>{key}</Key><UploadId>up{counter}</UploadId>"))

    def do_POST(self):
        u = urlparse(self.path)
        q = parse_qs(u.query, keep_blank_values=True)
        key = u.path.split("/", 1)[1] if "/" in u.path else ""
        body = self._body()
        del body
        with STORE.lock:
            STORE.requests.append(("POST", self.path))
            if "uploads" in q:
                STORE.counter += 1
                upid = f"up{STORE.counter}"
                STORE.mpu[upid] = {"sizes": [], "order": [],
                                   "h": hashlib.sha256(), "cur": None}
                stored = None
            elif "uploadId" in q:
                STORE.mpu_complete(key, q["uploadId"][0])
                Concurrency.complete(q["uploadId"][0])
                stored = True
            else:
                stored = None
                upid = None
        if "uploads" in q:
            self._send(200, _xml("InitiateMultipartUploadResult",
                                 f"<Key>{key}</Key><UploadId>{upid}</UploadId>"))
            return
        if stored:
            self._send(200, _xml("CompleteMultipartUploadResult",
                                 f"<ETag>\"done\"</ETag>"))
            return
        self._send(400, _xml("Error", "<Code>Unsupported</Code>"))

    def do_DELETE(self):
        u = urlparse(self.path)
        q = parse_qs(u.query, keep_blank_values=True)
        if "uploadId" in q:
            Concurrency.complete(q["uploadId"][0])
            with STORE.lock:
                STORE.mpu.pop(q["uploadId"][0], None)
            self._send(204, b"")
            return
        self._send(204, b"")


class FrigateHandler(BaseHTTPRequestHandler):
    """Serves a synthetic clip of a configurable size, chunked."""
    protocol_version = "HTTP/1.1"
    clip_bytes = 4 * 1024 * 1024
    segments_per_hour = 60
    stall_first_byte = 0.0
    chunk_delay = 0.0
    n_cameras = 2

    # Position-tagged payload: an all-zero clip would let a part overlap, gap
    # or boundary shift reach the wire with an identical digest, which is the
    # one bug class the byte-identity check exists to catch. Generated per
    # HTTP chunk from its absolute offset (no whole-clip buffer: the server
    # shares this process with the measured RSS). The pattern is 31 bytes --
    # not a power of two, so it never aligns with a 5 MiB part boundary and a
    # shifted or duplicated part changes the digest -- plus an absolute offset
    # at each end of every chunk, so a gap shows up even in the repeated part.
    PIECE = 256 * 1024
    PATTERN_LEN = 31

    @classmethod
    def chunk_at(cls, offset, n):
        pattern = hashlib.blake2b(
            (offset // cls.PIECE).to_bytes(8, "big"),
            digest_size=cls.PATTERN_LEN, person=b"memprobe").digest()
        reps = n // cls.PATTERN_LEN + 1
        out = bytearray(pattern * reps)[:n]
        out[:8] = offset.to_bytes(8, "little")
        out[n - 8:n] = (offset + n).to_bytes(8, "little")
        return bytes(out)

    def log_message(self, *args):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        if self.chunk_delay:
            time.sleep(self.chunk_delay)
        if u.path == "/api/config":
            cams = ",".join(f'"cam{i}": {{}}' for i in range(self.n_cameras))
            self._send(200, f'{{"cameras": {{{cams}}}}}'.encode())
            return
        if re.match(r"^/api/[^/]+/recordings$", u.path):
            body = self._segments(u)
            self._send(200, body)
            return
        m = re.match(r"^/api/([^/]+)/start/[\d.]+/end/[\d.]+/clip\.mp4$", u.path)
        if m:
            self._clip()
            return
        self._send(404, b'{"error":"nope"}')

    def _segments(self, u):
        q = parse_qs(u.query, keep_blank_values=True)
        after = float((q.get("after") or ["0"])[0])
        before = float((q.get("before") or ["0"])[0])
        span = max(before - after, 1)
        n = int(self.segments_per_hour * span / 3600) or 1
        out = []
        t = after
        for i in range(n):
            out.append({"start_time": t, "end_time": t + 10.0, "motion": 1,
                        "objects": [], "duration": 10.0, "segment_size": 1000,
                        "id": f"seg{i}"})
            t += 120.0  # > max_gap: each segment becomes its own clip
        return json.dumps(out).encode()

    def _send(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _clip(self):
        # chunked, no Content-Length: exactly how Frigate streams an export
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        self.stream_clip(self.wfile.write)
        self.wfile.write(b"0\r\n\r\n")

    def stream_clip(self, write):
        """The clip body: PIECE-sized writes plus the trailing mfra box.

        Exposed so a verifier can rebuild the exact expected byte stream.
        """
        remaining = self.clip_bytes
        offset = 0
        while remaining > 0:
            n = min(self.PIECE, remaining)
            write(b"%x\r\n" % n + self.chunk_at(offset, n) + b"\r\n")
            offset += n
            remaining -= n
            if self.chunk_delay:
                time.sleep(self.chunk_delay)
        # a well-formed mfra box (declared size == bytes to EOF), the shape
        # ffmpeg's frag_keyframe export ends with, so the completeness check
        # sees a complete stream
        import struct
        tail = struct.pack(">I", 16) + b"mfra" + b"\0" * 8
        write(b"%x\r\n" % len(tail) + tail + b"\r\n")

    def expected_digest(self):
        """sha256 over the clip body the exporter must receive, in order."""
        h = hashlib.sha256()
        self.stream_clip(lambda frame: h.update(
            frame.split(b"\r\n", 1)[1][:-2]))
        return h.hexdigest()


def serve(handler_cls, host="127.0.0.1"):
    srv = ThreadingHTTPServer((host, 0), handler_cls)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# ---------------------------------------------------------------- child mode

def self_usage():
    """CPU, minor faults and RSS of THIS process, for the fake-services child
    to report through /__admin. The sandbox may hide /proc/<pid> from other
    processes, so the child reports about itself; the probe never reads the
    child's /proc directly.
    """
    import resource
    r = resource.getrusage(resource.RUSAGE_SELF)
    rss_kib = 0
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_kib = int(line.split()[1])
                    break
    except OSError:
        pass
    return {"utime": r.ru_utime, "stime": r.ru_stime, "minflt": r.ru_minflt,
            "rss_kib": rss_kib}


def spawn(clip_bytes, segments_per_hour, n_cameras, throttle_ms=0.0):
    """Start the two fakes in a child process; return (proc, s3_port, f_port).

    The fakes run out-of-process so that measured RSS covers the archiver
    alone: payload generation and body hashing are not the uploader's memory
    and must not be charged to it or share its allocator.
    """
    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__),
         str(clip_bytes), str(segments_per_hour), str(n_cameras),
         str(throttle_ms)],
        stdout=subprocess.PIPE, text=True)
    first = proc.stdout.readline().split()
    if len(first) != 3 or first[0] != "PORTS":
        proc.kill()
        raise RuntimeError(f"fake services did not report ports: {first!r}")
    return proc, int(first[1]), int(first[2])


def admin(s3_port, full=False):
    """Fetch the fake S3's status dict (objects count, or full integrity)."""
    url = f"http://127.0.0.1:{s3_port}/__admin" + ("?full=1" if full else "")
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


if __name__ == "__main__":
    FrigateHandler.clip_bytes = int(float(sys.argv[1]))
    FrigateHandler.segments_per_hour = int(sys.argv[2])
    FrigateHandler.n_cameras = int(sys.argv[3])
    S3Handler.throttle_sleep = (float(sys.argv[4]) if len(sys.argv) > 4
                                else 0.0) / 1000.0
    _s3 = serve(S3Handler)
    _f = serve(FrigateHandler)
    print(f"PORTS {_s3.server_address[1]} {_f.server_address[1]}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
