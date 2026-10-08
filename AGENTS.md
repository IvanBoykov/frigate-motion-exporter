# frigate-motion-exporter

Export motion-only clips out of a Frigate NVR instance.

## Credentials / environment

Secrets live in `.secrets` at the repo root (git-ignored, mode 600). Never inline them
into tracked files, commits, or docs - this repository is published, and anything
committed here is public.

- Frigate API: your own deployment, reached over HTTPS with HTTP Basic Auth.
  Credentials go in `.secrets` as `FRIGATE_USER` / `FRIGATE_PASSWORD` - the names the
  code reads.
- Auth behavior to expect from a proxied deployment: plain `http://` answers `302` to
  HTTPS, and an unauthenticated API request answers `302` to the auth layer rather
  than `401`, so a `302` from a 200-expecting call means missing credentials.
- Cameras are whatever your `/api/config` returns; the archiver discovers them and
  never hardcodes a name. Deployment details (version, retention settings, camera
  list) belong in your own notes, not in this file.

## Git remote

`origin` is a self-hosted Gitea instance for private work. The authenticated clone URL
lives in `.secrets` as `GIT_REPO_URL` - never write it into a tracked file.

That URL must be used **without** a trailing `.git`: with the suffix this Gitea answers
`repository not found`. The default branch is `master`. The public mirror on GitHub is
published from the `github-ready` branch, which carries a single history-free commit -
the private history here contains personal hostnames. See *Publishing* below.

## Frigate API notes

- `GET /api/{camera}/recordings?after=<ts>&before=<ts>` returns recording segments.
  **`after`/`before` are in seconds**, not milliseconds - epoch-ms silently returns an
  empty list instead of an error. Verify any timestamp unit assumption against real data.
- Segment objects expose `start_time`, `end_time` (float seconds) and `motion`
  (number of motion-detecting segments / motion score) used to decide motion presence.
- `GET /api/{camera}/start/{start}/end/{end}/clip.mp4` streams an MP4 and works with
  Basic Auth. Pass integer timestamps (`math.ceil`), as the code does - never a raw
  float formatted with `{ts:g}`: that yields `1.79042e+09`, which Frigate silently
  rounds to the nearest second and answers with `200 OK` and a **wrong-window clip**,
  not an error. A zero-byte export does happen on its own
  when ffmpeg dies before its first packet - HTTP 200, empty body, headers already
  committed - which is why the uploader treats an empty body as a fault.
- `GET /api/config` lists cameras under the `cameras` key. `/api/version` answers the
  full build string (for example `0.17.0-<hash>`), while `/api/config` reports only a
  short `0.17-0` form - compare against the former when a version matters.
- The recordings window moves with retention: `/api/recordings/start/0/end/<now>`
  returns only what is still stored, so a "everything available" query answers a
  rolling window (a few hundred segments per camera-hour is normal for a 10 s
  segment size) and older footage is gone rather than reported as an error.

## Repository layout

All application modules live in `src/`; tests, docs, and packaging files stay at the
repository root, and `conftest.py` puts `src/` on `sys.path` so `pytest` resolves the
modules from either location. The Docker image copies `src/` to the working directory,
so inside the image the module names are unchanged (`python frigate_s3_archiver.py`).

- `src/frigate_s3_archiver.py` - the archiver: motion-interval
  detection plus streaming each clip straight into S3 with multipart upload and
  resuming from a watermark derived from S3 keys (`get_last_processed_time`), so
  it is stateless apart from S3 itself. (An earlier local-disk prototype,
  `download.py`, was removed once this became the service.)
- `src/metrics.py` - Prometheus definitions, the error-kind vocabulary, and the `/metrics`
  listener. The archiver imports it and calls it; no metric is defined elsewhere.
- `src/mp4_tail.py` - decides whether the tail of an MP4 proves the export finished:
  searches backwards from EOF for an `mfra`/`moov`/`mdat` signature and accepts the
  stream only when that box's declared size lands exactly on EOF. Used to tell a
  complete clip from one cut mid-export; no metric or S3 code.
- `src/logging_setup.py` - logging configuration: leveled output on stderr, a
  `camera` field rendered by the formatter (bound through a `contextvars`
  variable, so per-camera context needs no parameter threading), and a JSON
  formatter selected by `LOG_JSON`. Call sites never change between formats.
- `src/config.py` - reads the tuning knobs into one frozen `Config` dataclass at
  startup. `load_config()` returns it; every value is parsed and range-checked
  there, so an invalid setting exits at startup with the variable name in the
  message instead of failing mid-clip. `ConfigError` is a `SystemExit`.
  It imports the `DEFAULT_*` constants from `frigate_s3_archiver` (one direction
  only - the archiver imports `config` inside `main()` to avoid a cycle).
  Two settings deliberately bypass it: `METRICS_PORT`/`METRICS_BIND` are read by
  `src/metrics.py` at bind time, and `make_frigate_client` falls back to
  `FRIGATE_USER`/`FRIGATE_PASSWORD` when called without explicit credentials.
- `conftest.py` - puts `src/` on `sys.path` for the test suite.
- `test_frigate_s3_archiver.py`, `test_logging_setup.py`, `test_metrics.py`,
  `test_mp4_tail.py`, `test_config.py` - see *Tests*.
- `Dockerfile` - two-stage build (`python:3.14-slim`): dependencies into an
  isolated prefix, then `src/` copied into `/app` and owned by the
  non-privileged `appuser` the container runs as.
  `.github/workflows/docker-publish.yml` builds the image for
  `linux/amd64,linux/arm64,linux/arm/v7` and pushes it to GHCR on pushes to the
  default branch, on `v*` tags, and on manual dispatch.
- `compose.yaml` runs the image with a required `env_file`; `compose.debug.yaml`
  is the same service with `debugpy` listening on 5678.
- `requirements.in` is the direct dependency list; `requirements.txt` is the
  pinned lock generated from it with `pip-compile`.
- `env.example` - the template for the `.env` both compose files require.
- `LICENSE` - GNU GPL v3.0 (see *Publishing* for why this license and what it
  obliges downstream users to do).

## Architecture of `src/frigate_s3_archiver.py`

- `make_frigate_client(base_url, username=None, password=None)` returns a prepared
  `{"base_url", "session"}` pair. Auth is attached only when a username is set, so the
  same code runs unauthenticated against a local Frigate on `http://localhost:5000`.
  Business functions take the client and never resolve a URL or auth themselves.
  Because every camera issues its requests from a `to_thread` worker and
  `requests.Session` is not thread-safe, `async_main` builds one
  `make_frigate_client_factory` from the config and each camera task calls it for a
  client with its own session; the factory still resolves the configuration once,
  outside the business functions.
- `main()` calls `config.load_config()` and hands the resulting object to
  `async_main(config)`, which passes every knob down as a plain argument. No business
  function reads the environment: the S3 client is still boto3's own chain (only
  `S3_BUCKET` is ours), and everything else arrives through `Config`.
- `async_main` starts the metrics exporter (`METRICS_PORT`, default 9108; binding it
  is intentionally fail-fast), reads cameras from `/api/config` through
  `fetch_cameras_with_retry` (12 tries 5 s apart - the only pre-supervisor Frigate
  call, and a Frigate that is still booting must not kill the process; after the
  budget startup fails), narrows them with `select_cameras` (`CAMERAS_INCLUDE` /
  `CAMERAS_EXCLUDE`, at most one), then runs one
  `supervise_camera` task per camera. A camera task that dies - Frigate down, S3
  rejecting, a bug in the pass - is restarted by *its own* supervisor with
  exponential backoff (`CAMERA_RESTART_BACKOFF_SECONDS` doubling to
  `CAMERA_RESTART_BACKOFF_MAX_SECONDS`, 5 s -> 300 s by default), so one broken camera
  keeps only its own watermark frozen (no hole is skipped over, its clips catch up on
  recovery) while the others keep archiving. The process exits only if a supervisor
  itself dies, which is a supervisor bug. An earlier design cancelled all survivors on
  the first camera exception; that made one dead camera stop the whole archive, which
  is worse than one camera lagging.
- The restart clock belongs to the supervisor: `run_camera` calls `on_pass_ok()`
  after each pass that reached Frigate and S3, and that is what resets the
  backoff - not the task merely being alive. A camera that crashes every minute
  backs off; one that recovered an hour ago restarts at the base delay.
- `run_camera` loops forever: S3 watermark -> `find_motion_intervals` -> `upload_intervals`
  -> rescan from the new watermark -> `IDLE_SLEEP_SECONDS` (60 s) pause when idle.
- The watermark is a pure logical boundary (the end of the last archived interval, and on
  cold resume the start of the newest clip in the bucket). The +2 s `next_after_ts` margin
  is applied only to the Frigate `after_ts` query, never to `min_start_ts`: a segment that
  straddles the boundary (query returns it via overlap) must pass the `seg_start <
  min_start_ts` filter, or the interval ending exactly on the boundary is dropped and its
  clip is lost forever. This was a real regression: the margin in `min_start_ts` silently
  ate the 10-minute chunk tail deferred by the previous pass.
- The cold-resume watermark being a clip *start* rather than the in-process *end* is a
  deliberate, safe regression: resuming early re-scans an already archived clip
  (`head_object` skips it) instead of risking a gap. The bucket-state enumeration in
  README.md ("Crash recovery") walks every state a bucket can be left in - including
  SIGKILL during PUT/multipart, where an unfinished multipart upload is invisible to
  listings, so a restart re-uploads the clip - and the invariants are pinned by the
  "Bucket-state recovery matrix" tests. The one accepted loss scenario is Frigate
  re-reporting an archived interval start with a *later* end: the key proves only the
  start, the shorter clip counts as archived, and the extra seconds are not re-fetched.
  A restart test pins this divergence so a future change cannot make it worse silently.
- Uploads run strictly in chronological order inside a camera pass and stop at the first
  failure (S3 errors and non-200 Frigate responses both propagate) - skipping a bad clip
  would leave a gap the watermark then jumps over.
- Buffering keeps the prototype's shape: an in-memory bytearray holds at most one
  `part_size` (default 5 MB) before the first multipart part is flushed, so peak
  clip memory is about `part_size` plus the separate completeness-tail window
  (`max(2 * mp4_tail.TAIL_WINDOW_BYTES, http_chunk_size)`, i.e. the larger of
  512 KiB and the HTTP chunk size, not 2x `part_size`). Anything larger than one
  part streams through multipart parts.
- Long blocking calls (`boto3`, the streamed download) run via `asyncio.to_thread` so the
  camera tasks overlap. Interval *detection* stays sequential per camera on purpose: the
  open-interval state machine depends on chunk order.
- `asyncio.to_thread` cannot cancel a running thread - cancelling the task only detaches
  the await - so `upload_clip_hybrid` passes a `threading.Event` into the worker and sets
  it when its own await is cancelled. `_upload_clip_hybrid_sync` checks that event at every
  chunk/part boundary and raises `CancelledError`, which lands in the multipart cleanup
  (`except BaseException`, not `except Exception`: `CancelledError` is a
  `BaseException`) so a cancelled upload aborts the multipart instead of orphaning it.
  Without this the abandoned worker kept downloading from Frigate and writing parts after
  the task was "cancelled" - and `gather` returns immediately, so nothing upstream noticed.
- Detection logic (`next_after_ts`, `split_interval_for_archive`, `find_motion_intervals`)
  came from a local-disk prototype and is unchanged in behavior. The differential test
  over 300 seeded random segment sets x 3 chunk configurations x 2 fetch regimes
  (range-honoring and duplicate-heavy overlap) that proved it ran against that
  prototype, which is not in this repository - so the equivalence it established is a
  historical fact, not something a run of `pytest` can re-check here. What the suite
  pins instead is the behavior itself. `max_gap_seconds=40` serves
  both gap roles on purpose: a mid-window gap over 40 s ends the current event and starts
  a new one, and at the window edge the same 40 s marks "too close to be a settled end".
- Deliberate behavior changes vs the prototype, all in the delivery path: upload/S3/Frigate
  errors and empty clips raise instead of being printed and counted as warnings, so the
  per-camera loop aborts instead of advancing the watermark over a hole; `upload_clip_hybrid`
  now returns `(is_new, was_truncated)` - the first value is True only for newly uploaded
  clips (False means "already in S3"), and `upload_intervals` returns
  `(uploaded, skipped, last_end_ts, truncated)`.

## Truncated clips

A Frigate clip export killed mid-stream simply stops delivering bytes: the response was
already 200, so completeness can only come from the stream itself. `mp4_tail` reads box
headers, never box contents: it searches backwards from EOF for an
`mfra`/`moov`/`mdat` signature, reads that box's declared size, and accepts the stream
only when the size lands exactly on EOF. A signature whose size disagrees is a random
byte match inside media data (or a box cut short), and the search continues to the next
candidate further back; only after the whole buffer is exhausted is the stream cut off.
`_upload_clip_hybrid_sync` keeps a rolling tail (`max(2 * mp4_tail.TAIL_WINDOW_BYTES,
http_chunk_size)`, so the final chunk always contributes a full
search window) and calls `mp4_tail.stream_is_complete()` at end-of-stream,
*before* `put_object` / `complete_multipart_upload`: a cut body must never be
archived as if it were whole, so it raises `metrics.ClipTruncatedError`
(kind `frigate_clip_truncated`).

The rule is format-agnostic - Frigate's fragmented exports end in the `mfra` FFmpeg
writes only after the last packet, a progressive MP4 ends in `moov`, a faststart one in
`mdat` - and was validated against real ffmpeg 7.1 transcodes of all these layouts and
218 real Frigate clips in the test bucket. Two accepted limits follow from box geometry:
a stream cut exactly at a box boundary (e.g. final `mdat` intact, `mfra` lost) reads
complete - the delivered bytes are whole boxes; and a complete stream whose final box
header lies outside the window reads truncated (a faststart file with a >256 KiB
trailing `mdat` - over-flagging costs a retry, Frigate's exports end in a small `mfra`
so the uploader window always sees it). A size-0 box ("to end of file" in ISO 14496-12)
is never accepted as an EOF match: any zero byte in media data would satisfy it.

`upload_clip_hybrid` retries that error and only that error (by type, which is why no other
failure can ever reach the truncated path) `DEFAULT_CLIP_RETRIES` times; the pause doubles from
`DEFAULT_CLIP_RETRY_BASE_DELAY_SECONDS` up to `DEFAULT_CLIP_RETRY_MAX_DELAY_SECONDS`
(`clip_retry_delay`). The same schedule retries the unavailable-window chain (the no-recordings
400 and the empty 200) - one shared budget, `CLIP_RETRIES`/`CLIP_RETRY_MAX_DELAY_SECONDS` -
sized (~310 s by default) to ride out a Frigate restart. When every attempt comes back cut,
a final fetch archives the partial body under `TRUNCATED_KEY_SUFFIX` (`-truncated.mp4`):
partial footage beats losing the event to a permanent export fault, and the suffix keeps the
hole visible. That final save passes `allow_truncated=True` - the body is known to be
cut off - and with `clip_retries=0` the error propagates instead, so opting out of
retries also opts out of truncated keys. Both closing objects (this truncated save and
the unavailable marker) additionally require the chain to be *pure*: `seen_faults` must
hold exactly one family, so a chain mixing truncated and unavailable answers raises
instead of sealing the window either way, and any 5xx/timeout/transport error escapes
the loop as a plain failure before a closing object is reachable.

An existing `*-truncated.mp4` counts as covering its window for *both* keys: Frigate's export
of that window is not going to improve, so re-fetching it every pass would stream the same
broken body forever. `parse_s3_clip_start` accepts the suffix, which keeps the watermark
advancing past a truncated clip. `make_s3_key(..., suffix=)` is the only place the suffix is
built.

Track durations from the file are deliberately never used as a length check: with `-c copy`
FFmpeg carries the input track durations over, so a 29.9 s export can advertise 37.7 s and a
duration comparison produces false truncations. A final box that reaches EOF is the
trustworthy signal, which is what `test_mp4_tail.py` pins (including a final box chopped
at every byte length).

## Metrics

`metrics.py` owns the exporter; the archiver only calls into it. Everything is on the
process-global `prometheus_client` registry, and `async_main` binds `METRICS_PORT`
(9108) on its own listener at startup.

- `clips_uploaded_total{camera}`, `clip_bytes_uploaded_total{camera}`,
  `clips_skipped_total{camera}` - counted where the write actually happened, so a
  skipped (already present) clip never inflates the upload counters.
- `clips_truncated_uploaded_total{camera}` - clips archived under a `*-truncated.mp4`
  key because every download attempt was cut inside its final box (see
  *Truncated clips*).
- `clips_unavailable_uploaded_total{camera}` - `*-no-recordings` markers written because
  every attempt for a window reported it unavailable, so the watermark is not pinned on
  footage Frigate can no longer export.
- `frigate_response_seconds{kind="json"}` wraps every Frigate JSON GET;
  `kind="clip_ttfb"` measures time to the first byte of a clip, i.e. Frigate's export
  start, and deliberately excludes the transfer, which is stream-bound.
- `errors_total{camera,kind}` - `error_kind()` maps an exception (walking `__cause__`,
  cycle-safe) onto a fixed vocabulary for our own errors; `ClientError` becomes
  `s3_<lowercased code>`, because S3 codes are worth alerting on individually and
  cannot be enumerated. Unrecognized errors are `unknown` - never dropped.
- `camera_restarts_total{camera}`, `camera_consecutive_failures{camera}`,
  `camera_task_running{camera}`, `camera_motion_intervals_found{camera}` - the alerting
  surface for the supervisor. The last one is a gauge of how many intervals the camera's
  most recent motion-interval search returned (0 when it found nothing, stale when the
  search itself failed), so "scanning but idle" reads differently from "not scanning".
  `camera_task_running` is set to 1 by `run_camera` itself, *after* the watermark
  read returns, and to 0 by the supervisor when the task ends. So it is 1 only for
  the states that actually archive (scanning, uploading, the normal idle sleep),
  and 0 while the supervisor backs off, while the task has just (re)started, and -
  the reason the set lives in the task rather than the supervisor - while the task
  hangs on a watermark read from an S3 that answers nothing. Setting it on task
  start would report "working" for a camera blocked before its first successful
  request. Consequence: the first scrape after a restart may show 0 for a healthy
  camera; that is the honest reading, and `camera_restarts_total` distinguishes it
  from a stalled one.

## Remaining gaps

Our code reads four S3 variables and nothing else: `S3_BUCKET` (no default - missing
it exits at startup), `S3_PART_SIZE`, `S3_PREFIX` and `S3_KEY_TIMEZONE`. The first
names where clips go, the other three shape what is written. Credentials, region,
endpoint and addressing style are not ours at all - they come from the standard boto3
chain (env vars, `~/.aws/credentials`, `~/.aws/config`, `AWS_PROFILE`,
`AWS_ENDPOINT_URL_S3`, `AWS_S3_ADDRESSING_STYLE`), documented in README.md. The time
budget is ours, not the chain's: `make_s3_client` pins `connect_timeout` (5 s),
`read_timeout` (8 s), `total_max_attempts` (10) and `retry_mode` (`standard`) on the
botocore config and registers a `needs-retry` hook that logs each retry, so one broken
S3 cannot hang a camera forever.
The Frigate side is verified end-to-end against
your own Frigate deployment. The S3 side is verified against `rclone serve s3` on
`127.0.0.1:9000` with throwaway test credentials and a local data directory, plus a
fault-injecting HTTP proxy in front of it.

## Verifying S3 fault behavior

Fake S3 clients cannot show what botocore actually retries - they never return a
503 or stall. Check it against a real endpoint instead: serve S3 with rclone, put
a proxy in front that answers 503 for a chosen range of request numbers
(`"A:B"` or `"A:B:CODE"` in a flag file, `Connection: close` on every reply so
boto3 reconnects), then upload a real Frigate clip through it and compare the
stored object's sha256 and ffprobe output with a fault-free baseline. Three things
are worth separating, because they cost different things:

- a blackholed endpoint costs `attempts x read_timeout`;
- a fast-failing outage costs only the backoff sum, so how long an outage can be
  ridden out is set by backoff, not by the timeout;
- permanent errors (403, `NoSuchBucket`) are not retried at all.

botocore re-sends a request body only when it can rewind it, so an upload path
that hands S3 a stream would fail on the first 5xx without one retry. Every body
in the upload path is `bytes` for that reason; a test pins it.

## Tests

`python3 -m pytest` - 204 tests, five modules, no mocks of the code under test.

`test_frigate_s3_archiver.py`: a real threaded HTTP server stands in for
Frigate and a fake S3 client records write order. Covers auth/no-auth clients,
startup camera discovery surviving a brief Frigate outage (scripted 503s,
injected sleep, the give-up budget), per-camera interval detection,
chronological upload out of shuffled input, fail-fast before a later clip,
resume-from-bucket, skip-if-present, and one supervised task per camera. Long events are covered too: 25 min of motion splits
into 10-minute chunks, a short tail near the scan boundary is deferred while the
event may still be running, the same tail is archived once the event settles or
once the scan window moves past it, and only the last interval is ever deferred.
The truncation path is covered at both levels: with retries a permanently cut
export is saved under `-truncated.mp4` (and a later complete attempt is not),
without retries it is a plain failure, an HTTP 500 never produces a truncated
key, and `run_camera` recovers past a permanently truncated camera instead of
re-fetching it every pass.

`test_metrics.py`: error-kind classification (including `__cause__` chains and a
cause cycle), the exporter's HTTP output, latency observations against the real
HTTP Frigate double, and the supervisor: restart counts, exponential backoff, the
backoff reset by a healthy pass, a task that returns counting as a failure,
cancel propagating to the camera task, and one broken camera not stopping
another. Camera labels are unique per test because the registry is
process-global.

`test_mp4_tail.py`: the last-box-at-EOF detector - synthetic byte-exact layouts of
all four final-box shapes (mfra, moov, trailing mdat, 64-bit size), cuts at every byte
inside the final box, a decoy signature inside media data that must be skipped, the
size-0 box, the too-short buffer, and real ffmpeg transcodes (Frigate's
frag_keyframe+empty_moov layout, progressive, faststart) checked complete-at-EOF and
truncated at cuts from 1 byte inside the trailer to deep inside a fragment.

`test_config.py`: the config layer against an explicit environ mapping -
defaults equal to the module constants, blank values treated as unset, every
rejected value naming its variable, `S3_PART_SIZE` refused below the multipart
minimum, and the two camera filters refused together. Also pins the key layout:
with no prefix set a key is byte-identical to one written before prefixes
existed (so an existing bucket keeps resuming), and build/parse round-trip only
inside one layout, which is what makes a changed `S3_PREFIX` show up as an empty
watermark lookup rather than a wrong one.

`test_logging_setup.py`: the logging layer - level parsing (the documented
spellings, `WARN` included, and an invented level refused), a level filtering what
reaches the stream, `configure_logging` replacing its previous handler rather than
stacking them, the text line carrying level and camera as fields (and no camera field
outside a camera context), the camera binding following its own task into worker
threads, JSON records including a traceback kept inside one field, and the
third-party DEBUG chatter staying out while the archiver's own lines pass.

Two plumbing tests in `test_frigate_s3_archiver.py` drive `run_camera` with a
non-default `max_gap_seconds` and a non-default `key_prefix` and assert the values reach
the requests and the keys - a knob that parses but is not threaded through would pass
every config test.

Verified live (not in CI): with a real HTTP Frigate double and real rclone S3,
closing the Frigate listener made both cameras crash and restart, exporting
`camera_task_running == 1` while they worked and `== 0` through the backoff
sleep, freezing their watermarks (no gap skipped), raising
`errors_total{kind="frigate_unreachable"}` and `camera_restarts_total`, while
the process stayed alive and kept serving `/metrics`. In a second run, one
camera aimed at a TCP black hole (connections accepted, never answered) reported
`camera_task_running == 0` for its whole wedged watermark read while the other
camera archived at `1` - the case the gauge placement in `run_camera` exists for.

Every test that drives `async_main` keeps the metrics listener off the fixed default
port - either by stubbing `start_metrics_server` or by setting `METRICS_PORT=0` and
letting the OS pick an ephemeral port - because `async_main` binds a real port and the
fixed 9108 default would collide with a previous run of the suite (or a running
archiver). One test pins the other half of that: the listener is closed on the
signal-stop path, so it does not outlive the run.

## Frigate API assumptions (verified against Frigate 0.17 sources)

The analysis below was checked against the `0.17` sources of
blakeblackshear/frigate, not the docs; behavior notes in this file were verified
against a 0.17 deployment.

Guarantees the archiver relies on:

- `/api/{camera}/recordings` (api/media.py `recordings`): overlap selection
  `end_time >= after AND start_time <= before`, `ORDER BY start_time`. Closed
  segments are immutable: a DB row is inserted when the segment file is moved
  into `/recordings` (record/maintainer.py `move_segment`) and is never updated
  afterwards - the interval-tail settlement logic is safe on this.
- `/api/.../clip.mp4` (api/media.py `recording_clip`): a concat of exactly the
  DB rows overlapping [start, end], re-muxed with ffmpeg `-c copy`. Returns
  HTTP 400 (JSON) when zero rows overlap - the archiver treats that as a fatal
  per-camera error, which is correct: the watermark must not jump the hole.
- Timestamps are unix seconds in the Frigate host clock domain; the archiver
  inherits that domain for the whole pipeline and cannot detect its skew.

What the API does NOT guarantee, but the archiver's logic assumes:

- Completeness of `/recordings` inside the queried window: rows appear only for
  what the recorder actually wrote. Cache segments dropped under maintainer
  overload (record/maintainer.py, "too slow"/"detection failed" unlink paths)
  leave neither rows nor files, so the archiver cannot distinguish "no motion"
  from "lost recording" - a >40 s gap simply ends the interval. The 0.17 API has
  no recordings-gap endpoint on this build (probed live 2026-10-05: global
  `/api/recordings/unavailable` answers 500, camera-scoped answers 404), so
  there is nothing to consume for gap detection yet.
- Integrity of the clip body: if ffmpeg dies mid-stream the response is already
  HTTP 200 (chunked, no Content-Length) and the stream just ends - a truncated
  clip looks like a successful upload. Solved by the tail check: a stream whose
  final box does not reach EOF is detected before the object is finalized, the
  window is retried, and only an exhaustively truncated export is archived under
  `-truncated.mp4` (see *Truncated clips*). `EmptyClipError` still covers the
  all-or-nothing case (ffmpeg dead before the first byte).
- Exact range: the endpoint floors in/outpoints with `int(...)` against
  `clip.start_time`, so a clip can lose up to 1 s of the requested tail
  (inpoint floors the other way, so the head is safe). Systematic, <=1 s/clip.
- Retention races: cleanup (hourly by default) unlinks files and deletes rows
  together; if it expires segments between the archiver's scan and its clip
  request, the clip call answers 400 -> per-camera restart -> the rescan no
  longer sees the interval. Self-healing and the footage was already gone.

Retention math for the recovery matrix, in terms of the Frigate settings it reads:
with `record.continuous.days=0`, `record.motion.days=35`, mode `motion` - non-motion
segments die within the hour, motion-bearing segments live 35 days, and the
window is computed from *now*, so Frigate-side loss equals downtime beyond
35 days. The archiver's own first-resume fallback (3 days) is the binding
constraint long before Frigate's retention: downtime > `first_run_lookback`
skips footage that Frigate still has. Raising `first_run_lookback_seconds` to
something under 35 days is the cheapest durability win available.

## Dependencies

- Python 3.14; see `requirements.txt` (`requests`, `boto3`,
  `prometheus-client`). The suite was run green on 3.14.6, and
  `.github/workflows/test.yml` runs it on 3.14 for every push. `requirements.txt` is a
  `pip-compile` lock pinned by Python 3.14, which matches the `python:3.14-slim` image;
  regenerate it from `requirements.in` with that same interpreter so the lock header
  and the runtime stay in step.
- Tests additionally need `pytest`.
- `ffmpeg`/`ffprobe` are installed here (`sudo apt-get update && sudo apt-get
  install -y ffmpeg`); used outside the test suite to confirm a fault-injected
  upload still stores a playable clip.

## Publishing

This tree is meant to be public. Before publishing (or re-publishing) a snapshot:

1. Check that no personal value came back:
   `git grep -nEI '<your-domain>|<your-git-host>|<camera-name>'` plus a
   search for real credentials. Nothing here should name a live host, camera, or
   bucket; `example.org`-style placeholders are what belongs in docs.
2. Scan **history**, not just the worktree - a deleted file is still in every older
   blob. `git grep` with a commit argument walks one tree, so it silently misses
   this; sweep the objects instead:
   `git rev-list --all --objects | awk '{print $1}' | sort -u | while read b; do
   git cat-file -t $b | grep -q blob && git cat-file blob $b | grep -aIl '<pattern>' /dev/stdin; done`.
   The last full sweep of all 58 blobs in this history found no credential, hostname,
   camera name or bucket name, so on the content side the history is publishable
   as-is. Re-run it after any commit that touches `.secrets`-adjacent material.
3. Commit metadata is published with the repository, and the owner has decided which
   identities appear there. All 14 commits on `master` are authored and committed as
   `Ivan Boykov`, 10 of them as `ivan+gh@boykov.org.ru` and 4 hand-written ones (the
   root `6280921` and three others) as the older `ivan@boykov.org.ru`. Where the agent
   actually contributed, credit is carried by the trailer
   `Co-authored-by: openhands <openhands@all-hands.dev>` rather than by the author
   field; a commit the owner wrote alone has no trailer and must not gain one. This
   email attribution is settled - do not re-raise it as a leak and do not invent a new
   agent author identity. Check the current state with
   `git log master --format='%ae %ce' | sort -u`.
   The rewrite that produced this was `git filter-branch` over the whole branch, which
   changed every SHA and needed a `--force-with-lease` push. Rewriting published
   metadata for cosmetic reasons breaks other clones and any image builds traceable to
   old SHAs, so treat it as a one-time correction, not routine cleanup. The old history
   survives locally under `refs/original/` until pruned; those refs are not on the
   remote, so remove them once the rewrite is confirmed good, or `git update-ref -d`
   them individually.
4. `.git/config` holds the remote URL with the Gitea credentials inline. That file is
   never pushed, but a copied or bundled working tree would carry it - clone fresh
   for the public push rather than copying the directory.
5. Keep `.secrets` git-ignored, and never add it to a commit or to the published
   branch. `.gitignore` covers `.secrets`, `.env`, `*.pem` and `*.key`, and
   `.dockerignore` lists `.secrets` too, so a broadened `COPY` could not pick it up.
6. `LICENSE` is GNU GPL v3.0, verbatim from the FSF (sha256
   `3972dc9744f6499f0f9b2dbf76696f2ae7ad8af9b23dde66d6af86c9dfb36986`), chosen because
   the owner wants commercial use allowed while keeping improvements in the mainline.
   It is **GPL-3.0-only**, not "or later": Section 14 of the license lets a recipient
   pick a later version only when the Program says so, and no source file here carries
   that notice, so the README says "v3.0" without an upgrade path. If a future
   GPL revision should be usable, the notice has to be added to the files deliberately.
   What it obliges: anyone distributing a modified version ships the source under the
   same terms; private and internal use carries no obligation at all. Serving a modified
   copy over a network does not count as distribution either, so a hosted fork owes
   nothing back - that is the SaaS gap AGPL-3.0 closes, so switch to AGPL if network
   hosts must release source. Two things a copyleft license does *not* buy: it does not
   force anyone to send you their patches (they may publish a fork you cannot read),
   and without a CLA your project cannot later relicense to, say, MIT, because every
   contributor keeps their own v3 grant. If contributing back matters, say so in
   `CONTRIBUTING.md` rather than expecting the license to enforce it.
   Licensing of the shipped dependencies was checked against their distributions:
   `requests` Apache-2.0, `boto3` Apache-2.0, `prometheus-client`
   Apache-2.0 AND BSD-2-Clause, and transitively MIT (`urllib3`, `six`, `jmespath`),
   BSD (`idna`), Apache-2.0 (`botocore`, `s3transfer`), dual Apache/BSD
   (`python-dateutil`) and MPL-2.0 (`certifi`) - all GPL-3 compatible, so the image
   stays distributable. Never paraphrase the license text; reference `LICENSE`.
   Source files carry no SPDX headers, so authorship travels only through git history
   and `LICENSE`. Per-file `SPDX-License-Identifier: GPL-3.0-only` headers are the
   belt-and-braces option if that ever matters.
7. `.github/workflows/test.yml` runs `pytest` on Python 3.14 for every push and pull
   request - 3.14 only, because it is the only version the project claims to support
   and the version the image and the lock use. `docker-publish.yml` builds and pushes
   the image. A public repository that advertises 204 tests should run them somewhere
   other than the author's machine.

## Text conventions

Every byte in this repository stays inside the ASCII range - code, comments,
docstrings, exception and `print` texts, and the docs. User-facing messages are
English. Typographic characters are written as ASCII (`-`, `->`, `>=`, `<=`):
editing tools here have corrupted UTF-8 into mojibake, and an ASCII-only rule
makes that class of damage impossible. Check a file with
`python3 -c "print(open('FILE').read().isascii())"`.
