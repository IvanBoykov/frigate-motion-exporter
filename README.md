# frigate-motion-exporter

Archives motion-only clips from a Frigate NVR into an S3-compatible bucket.

Frigate deletes its own recordings after a retention period, and its database is
the only index - anything older than that is simply gone. This script continuously
copies every motion-bearing interval out of Frigate and into S3 before retention
expires it, so footage stays queryable and playable independently of the NVR. Run
it when you want motion footage kept for as long as the bucket is kept, without
extending Frigate's disk retention.

`frigate_s3_archiver.py` discovers cameras via Frigate's `/api/config`, then runs
one worker per camera: it resumes from a watermark derived from the newest clip
key in the bucket, detects motion intervals, splits long events into 10-minute
chunks, and streams each clip into S3 (single `PUT` or multipart) in strict
chronological order. Any Frigate or S3 error aborts the *camera* instead of
advancing its watermark over a hole, and its supervisor restarts just that
camera with an exponential backoff - one dead camera never stops the others and
never kills the process.

Run:

```bash
pip install -r requirements.txt

export FRIGATE_URL=https://frigate.example.org
export FRIGATE_USER=archiver FRIGATE_PASSWORD=secret
export AWS_ACCESS_KEY_ID=AKIAXXXX AWS_SECRET_ACCESS_KEY=yyyy
export AWS_REGION=us-east-1
# non-AWS endpoint (MinIO, Yandex Object Storage, R2, ...), if any:
# export AWS_ENDPOINT_URL_S3=https://s3.example.internal
export S3_BUCKET=my-motion-archive

python3 frigate_s3_archiver.py
```

Every setting is an environment variable; there is no config file. The full list,
with defaults and examples, is under [Configuration](#configuration).

With Docker, put the same variables in `.env` (copy `env.example`) — both
`compose.yaml` and `compose.debug.yaml` load it as a required `env_file`:
without the file compose refuses to start, because `S3_BUCKET` has no
default. Compose also auto-loads a shell-style `.env` from this directory,
so the file serves both paths:

```bash
cp env.example .env
docker compose up -d --build
```

Prometheus metrics are served on `:9108/metrics` (see [Metrics](#metrics)).

SIGINT (Ctrl-C) and SIGTERM (`docker stop`, systemd) shut the archiver down
gracefully: camera tasks are cancelled at their current await point, the
metrics listener closes, and the process exits 0 without a traceback. A
second signal kills a wedged shutdown outright.

Tests: `pip install pytest && python3 -m pytest`

## Guarantees and requirements

The guarantees below hold when the requirements below hold. Everything the script
does on its own initiative - retries, markers, supervisor restarts - is an attempt
to keep these guarantees through faults, not a substitute for meeting the
requirements.

Requirements:

- **Runtime:** Python 3.13 and the packages in `requirements.txt`. The script keeps
  no local state: no scratch disk, no database, nothing to back up.
- **Frigate:** reachable over HTTP(S), with Basic Auth credentials if the deployment
  has an auth layer. Behavior-verified against 0.17.0; the *Frigate API assumptions*
  section of `AGENTS.md` lists the server guarantees the logic leans on (seconds-based
  `after`/`before` parameters, overlap-based segment selection, immutable closed
  segments, `mfra`-terminated fragmented exports).
- **Retention covers planned downtime.** Frigate can only export what it still
  holds. `FIRST_RUN_LOOKBACK_SECONDS` (3 days by default) is the cold-resume window:
  footage older than that at startup is out of scope even if Frigate still has it.
  Set it below Frigate's motion retention (`record.motion.days`), never above it.
- **S3:** a bucket the credentials can `ListObjectsV2` by prefix, `HeadObject`,
  `PutObject`, and multipart create/upload/complete/abort against. The tool owns
  `<S3_PREFIX><camera>/` outright - the watermark is derived from those keys - so no
  foreign objects belong under the archive prefix.
- **One instance per (bucket, prefix).** Two instances archiving the same camera into
  the same prefix race on the same keys.
- **Synchronized clocks.** Segment timestamps, clip windows, and key dates all live in
  the Frigate host's clock domain; the script inherits that domain and cannot detect
  its skew. Keep the Frigate host and the archiver host on NTP.
- **`S3_PREFIX` and `S3_KEY_TIMEZONE` never change once the bucket holds objects** -
  see the note in the S3 section; a change makes the existing archive invisible to the
  watermark listing and the run resumes as if the bucket were empty.

Guaranteed when those hold:

- **No watermark holes.** Uploads run in strict chronological order per camera, and
  the watermark advances only over footage that was successfully stored - or, for a
  window proven unrecoverable, over a marker that records the loss (see *Truncated
  clips*). A Frigate or S3 failure aborts that camera, which resumes from the same
  boundary instead of skipping past the hole.
- **At-least-once coverage.** After an interruption the newest clip's window is
  re-scanned, and objects that already exist are skipped rather than rewritten. No
  footage inside the scanned range is written twice under two keys.
- **No silently truncated clips.** A stream whose final MP4 box does not reach EOF is
  never stored as a normal clip; the window is re-fetched, and only an export cut off
  on every attempt is stored, under a `*-truncated.mp4` key whose name declares it.
- **Camera isolation.** One camera's failure never stops another camera, never kills
  the process, and never moves another camera's watermark.
- **Crash-resumability from the bucket alone.** Any kill signal at any point -
  mid-PUT, mid-multipart, mid-scan - leaves a bucket state covered by the *Crash
  recovery* table; its one accepted loss scenario is documented there.

Out of scope by design:

- Footage outside the startup lookbacks (see the retention requirement above and the
  crash-recovery matrix).
- The newest footage in flight: an interval whose event may still be running is
  deferred to the next pass, so the archive trails `now` by roughly
  `IDLE_SLEEP_SECONDS` plus the event tail.
- Excluding non-motion frames from inside an interval. Frigate re-muxes whole
  recording segments, so where a deployment also retains continuous segments
  (`record.continuous.days > 0`), a clip's interior can include non-motion footage
  even though the interval bounds come from motion. With `continuous.days=0` - the
  configuration this was verified against - segments without motion are deleted
  within the hour and cannot appear.
- Sub-second tail precision. Frigate floors the clip outpoint, so an export can end
  up to ~1 s earlier than the requested window.
- Metric history across restarts. `prometheus_client` counters live in the process,
  so a restart resets them; alert on rate changes rather than totals.

## Configuration

All configuration is via environment variables; the script has no config file of
its own.

### Frigate

| Variable | Meaning | Default |
| --- | --- | --- |
| `FRIGATE_URL` | Base URL, e.g. `https://frigate.example.org` | `http://localhost:5000` |
| `FRIGATE_USER` | HTTP Basic Auth username | unset - no auth (plain localhost Frigate) |
| `FRIGATE_PASSWORD` | HTTP Basic Auth password | unset |

Auth is attached only when `FRIGATE_USER` is set, so the same build works against
an authenticated reverse proxy and an open localhost instance.

Cameras are discovered from `/api/config`; by default every configured camera is
archived. Discovery is the only Frigate call made before the per-camera
supervisors exist, so it is retried: 12 attempts spaced 5 seconds apart, which
outlasts a Frigate or reverse-proxy restart that overlaps this process starting.
After the budget the error is raised and startup fails - an endpoint unreachable
for a full minute is more likely a wrong `FRIGATE_URL` than a transient fault.
Exactly one of these two may be set - setting both is a startup error rather
than a precedence rule, because which cameras get archived should never be
decided by an implicit ordering:

| Variable | Meaning |
| --- | --- |
| `CAMERAS_INCLUDE` | Regex; archive only cameras whose name matches (`re.search`) |
| `CAMERAS_EXCLUDE` | Regex; archive every camera except those matching |

An invalid regex fails at startup and names the variable.

### Metrics

| Variable | Meaning | Default |
| --- | --- | --- |
| `METRICS_PORT` | TCP port serving `/metrics` | `9108` |
| `METRICS_BIND` | Address to bind the metrics listener to | `0.0.0.0` |

The exporter is a separate listener, so scrapes cannot be queued behind
archiving work, and a metrics port that cannot be bound fails the process at
startup rather than silently disabling observability.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `clips_uploaded_total` | Counter | `camera` | Clips newly written to S3 |
| `clip_bytes_uploaded_total` | Counter | `camera` | Bytes of those clips |
| `clips_skipped_total` | Counter | `camera` | Clips already in S3 (resume/idle overlap) |
| `clips_truncated_uploaded_total` | Counter | `camera` | Clips saved as `*-truncated.mp4` because every Frigate download attempt arrived cut off |
| `clips_unavailable_uploaded_total` | Counter | `camera` | `*-no-recordings` markers written because every attempt for a window reported it unavailable (see [Truncated clips](#truncated-clips)) |
| `frigate_response_seconds` | Histogram | `kind=json\|clip_ttfb` | Frigate latency |
| `errors_total` | Counter | `camera`, `kind` | Failures, by error kind |
| `camera_restarts_total` | Counter | `camera` | Times a camera task died and was restarted |
| `camera_consecutive_failures` | Gauge | `camera` | Crash/restart count since the last healthy pass |
| `camera_task_running` | Gauge | `camera` | 1 while the camera task works (watermark resolved); 0 at startup, on a wedged watermark read, during backoff |

`frigate_response_seconds{kind="json"}` covers the metadata requests
(`/api/config`, `/api/{camera}/recordings`); `kind="clip_ttfb"` measures the time
until the first byte of a clip, which is when Frigate starts exporting it -
the export itself then streams, so the histogram deliberately does not include
the transfer.

`errors_total{kind}` uses a fixed vocabulary for everything we classify
(`frigate_timeout`, `frigate_unreachable`, `frigate_http_4xx`,
`frigate_http_5xx`, `frigate_short_response`, `frigate_empty_clip`,
`frigate_clip_truncated`, `frigate_no_recordings`, `frigate_bad_payload`, `timeout`, `task_returned`, `cancelled`, `unknown`, and the S3 transport
kinds `s3_timeout`, `s3_unreachable`, `s3_connection_reset`,
`s3_short_response`); an S3 API error carries the code S3 returned
(`s3_accessdenied`, `s3_nosuchbucket`, `s3_slowdown`, ...), because those are worth
alerting on individually and cannot be enumerated. An error that matches nothing
is `unknown` - counted, never dropped.

Useful alerts:

```promql
# a camera is not archiving: its task is wedged on S3, down, or the supervisor
# is between restarts
min_over_time(camera_task_running[5m]) == 0

# a camera is crash-looping (the gauge resets on the next healthy pass)
camera_consecutive_failures > 3

# S3 rejected our credentials or the bucket
increase(errors_total{kind=~"s3_accessdenied|s3_nosuchbucket"}[5m]) > 0

# Frigate is slow to answer metadata queries
histogram_quantile(0.95, rate(frigate_response_seconds_bucket{kind="json"}[5m])) > 2
```

`camera_task_running` answers "is this camera's task doing work right now". The
task sets it to 1 itself, once its watermark read has returned - so the three
states where nothing is being archived all read as 0: the supervisor waiting out
a backoff, a task wedged on an S3 that answers nothing (a hung watermark listing
holds up the camera for the whole boto3 budget without any error), and a task
that has not started at all. The first scrape after a restart can therefore
still show 0 while the camera is healthy but not yet working; the gauge says
nothing is archived yet, which is true.

### S3

The script uses the default `boto3` client chain - the standard AWS credential
resolution order. No S3 parameters are read or parsed by our code, so anything
`aws configure` / the AWS SDK understands works as-is, with our env variables as
the one exception below (`S3_BUCKET`).

Required:

| Variable | Meaning |
| --- | --- |
| `S3_BUCKET` | Target bucket. There is no default; the script exits with a clear error if it is unset. |

Optional, affecting where and how objects are written:

| Variable | Meaning | Default |
| --- | --- | --- |
| `S3_PREFIX` | Key prefix, e.g. `frigate` -> `frigate/cam/...`. Normalized: no leading slash, exactly one trailing slash | unset - keys start at the camera name |
| `S3_KEY_TIMEZONE` | IANA zone the date/hour folder in the key is rendered in | `UTC` |
| `S3_PART_SIZE` | Multipart part size in bytes; must be at least the 5 MiB S3 minimum | `5242880` |

Credentials, region, endpoint - standard AWS/boto3 chain, first match wins:

1. Process environment:
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`,
   `AWS_DEFAULT_REGION` (or `AWS_REGION`).
2. Profile files written by AWS CLI: `~/.aws/credentials` and `~/.aws/config`
   (selected with `AWS_PROFILE`, default profile otherwise).
3. Other standard sources (instance profiles, container credentials, web identity).

Custom endpoints (Yandex Object Storage, MinIO, Cloudflare R2, ...):

- `AWS_ENDPOINT_URL_S3=https://storage.example.internal` - service-specific
  endpoint override, honored by current boto3 (and AWS CLI v2). `AWS_ENDPOINT_URL`
  also works as the global fallback.
- Or in the profile: `endpoint_url = https://storage.example.internal` under
  `[default]` (or `[profile name]`) in `~/.aws/config`.

Timeouts and retries are not configurable: they are pinned on the S3 client in
`make_s3_client()` so that one broken S3 cannot hang the archiver forever. botocore
standard retry mode retries 5xx, timeouts and throttling with capped exponential
backoff, and does not retry permanent errors (403, `NoSuchBucket`, bad credentials),
which still abort immediately. Worst case for a single S3 operation is about
2.5 minutes - `attempts x read_timeout + backoff`; measured against an S3 that
accepts connections but never answers, the archiver gives up in ~100-110 s instead
of the ~310 s an untuned client takes. Transient 5xx are ridden out for up to about
a minute, which is what the same backoff budget buys.

Path vs virtual-host addressing:

- boto3 resolves this itself (`addressing_style = auto`): AWS-style endpoints use
  virtual-host style (`https://bucket.host/key`), custom endpoints where bucket
  names cannot be subdomains fall back to path style
  (`https://host/bucket/key`). Verified against botocore 1.43 with a custom
  endpoint - the default already produced path style.
- To force one: `AWS_S3_ADDRESSING_STYLE=path` (or `vhost`), or in the profile:

  ```ini
  [default]
  region = ru-central-1
  endpoint_url = https://storage.yandexcloud.net
  s3 =
    addressing_style = path
  ```

Profile example equivalent to the full env set:

```ini
# ~/.aws/credentials
[mystorage]
aws_access_key_id = YCAJ...
aws_secret_access_key = ...

# ~/.aws/config
[profile mystorage]
region = ru-central-1
endpoint_url = https://storage.yandexcloud.net
s3 =
  addressing_style = path
```

```bash
export AWS_PROFILE=mystorage
export S3_BUCKET=my-motion-archive
python3 frigate_s3_archiver.py
```

At startup the script prints the effective bucket, key prefix, endpoint and
region (`S3: bucket=... prefix=... endpoint=... region=...`) so the resolved
configuration is visible in the log.

Clip keys have the shape `[<prefix>/]<camera>/YYYY/MM/DD/HH/MM-SS.mp4`, where the
timestamp is the clip start in the configured key zone and is also what the
watermark resumes from.

`S3_PREFIX` and `S3_KEY_TIMEZONE` must stay fixed once the bucket holds objects:
the watermark is rebuilt by reading existing keys back, so a changed prefix or
zone makes the previous archive invisible to the listing and the run resumes as
if the bucket were empty - which skips footage rather than corrupting it.

### Timing and recovery tuning

Defaults are chosen for the deployed setup; each of these changes how the
archiver behaves rather than where it writes. A malformed or out-of-range value
names the variable and exits before the first request is sent.

| Variable | Meaning | Default |
| --- | --- | --- |
| `IDLE_SLEEP_SECONDS` | Pause after a pass that found nothing to archive | `60` |
| `HTTP_TIMEOUT` | Seconds allowed for one Frigate request, including streaming a whole clip | `300` |
| `MAX_GAP_SECONDS` | Largest gap between motion segments still treated as one event. Raise it when a camera drops segments often, so a brief hole does not end the event and start a new one; gaps wider than this really are two events, and bridging them would archive a clip containing a hole | `40` |
| `WATERMARK_LOOKBACK_SECONDS` | How far back the startup listing looks to find the newest clip for a camera. Must cover the longest planned downtime, otherwise the archive resumes later than it should and footage is skipped | `86400` |
| `FIRST_RUN_LOOKBACK_SECONDS` | Where a camera with no clips in the bucket starts. Kept at or beyond the Frigate recording retention | `259200` |
| `CLIP_RETRIES` | Re-fetch attempts for a failed clip window, shared by all three chains: a stream cut inside its final box, a "no recordings" 400, and an empty 200. `0` acts on the first answer: a truncated export is reported as an error instead of being re-requested (and produces no `-truncated.mp4` key), an unavailable window is closed by its marker without retrying | `6` |
| `CLIP_RETRY_MAX_DELAY_SECONDS` | Ceiling for the pause between those attempts: it starts at 10 s and doubles until it saturates here. The defaults span ~310 s across all retries - a budget sized to ride out a Frigate restart without sealing windows | `80` |
| `CAMERA_RESTART_BACKOFF_SECONDS` | First delay before a crashed camera task is restarted; doubles per consecutive crash | `5` |
| `CAMERA_RESTART_BACKOFF_MAX_SECONDS` | Ceiling on that delay - effectively how long the archiver waits out a Frigate outage. With a 35-day retention it can safely be set to hours | `300` |

`WATERMARK_LOOKBACK_SECONDS` and `FIRST_RUN_LOOKBACK_SECONDS` are two different
windows, not two settings for one thing. The first is a search bound: if it finds
no clip, the watermark cannot be trusted and the second applies - the starting
point for a camera whose bucket prefix is empty. Because the fallback replaces a
lookback that failed rather than one that found a recent clip, downtime longer
than the first window costs re-scanning, not coverage; downtime longer than the
second costs footage.

### Crash recovery

What a restart does depends only on the objects in the bucket - the script keeps
no local state. A key names a clip by its start; the clip's end is not stored,
so after a restart the archive resumes from the *start* of the newest clip, one
clip earlier than the in-process watermark was. That early resume is what makes
every interruption recoverable: the scan re-derives the intervals, the
`head_object` check skips what is already archived, and only the missing
footage is fetched.

Bucket state at exit -> behavior on the next start:

| Bucket state | Restart behavior | Lost footage |
| --- | --- | --- |
| Empty (first run, or only unparseable keys) | Scan the last `first_run_lookback_seconds` (3 days) | nothing newer than the window; older footage is out of scope by design |
| Newest clip within `watermark_lookback_seconds` (24 h) | Resume from its start; the overlapping clip is skipped by `head_object` | none |
| Newest clip older than 24 h (long downtime) | Treated as no clips: resume from the 3-day first-run window, which is wider than the lookback, so the gap between the newest clip and now is re-scanned | none, provided downtime < 3 days |
| Downtime longer than the 3-day first-run window | Everything between the last clip and `now - first_run_lookback_seconds` is never scanned, even though Frigate may still hold it (this instance keeps motion segments 35 days) | footage older than 3 days of downtime - raise `first_run_lookback_seconds` if the host can be offline longer |
| Killed during a scan | Nothing was written; the pass is redone | none |
| Killed between clips | Resume from the newest clip's start | none |
| Killed during a small clip (single PUT) | S3 PUT is atomic: the object is complete or absent; re-uploaded | none |
| Killed during a large clip (multipart) | Parts are invisible to a listing until `complete_multipart_upload`; the clip is re-uploaded from the start | none - but the abandoned upload lingers as invisible billable storage until a bucket lifecycle rule removes it, so configure one |
| Killed after the last upload, before the loop continued | Normal resume from the newest clip's start | none |

One accepted limitation: an existing object at a key is the only proof that a
clip is archived, and the key identifies a clip by start alone. If Frigate ever
re-reported the same interval start with a later end, the shorter existing clip
would be considered sufficient and the extra seconds would not be archived. In
practice Frigate freezes a recording segment once a newer segment exists, and
the interval tail is only finalized when a `motion=0` segment already proves the
event ended - so a closed interval cannot grow after the fact. The restart path
is pinned by tests in `test_frigate_s3_archiver.py`
("Bucket-state recovery matrix").

### Truncated clips

A clip export killed mid-stream simply stops delivering bytes while the
response was already HTTP 200, so the uploader keeps a rolling tail of the
stream and checks it at end-of-stream: an MP4 is complete only when it ends on
an intact box. The check searches backwards from the end of the stream for a
`mfra`/`moov`/`mdat` box header (Frigate's fragmented exports end in the
`mfra` index FFmpeg writes only after the last packet) and demands that the
box's declared size lands exactly on the end of the stream; a signature whose
size disagrees is random bytes inside media data, and the search continues
further back. A stream that fails is a Frigate fault, not a complete clip, so
the body is never uploaded as if it were one. The window is re-requested
instead (6 retries by default; the pause starts at 10 s and doubles up to 80 s,
so every attempt fits inside ~5 minutes - long enough for a Frigate restart).
If every attempt - and
only this failure, never an HTTP/S3 error - comes back cut off, the partial body
is archived under a `*-truncated.mp4` key: partial footage beats losing the
event to a permanent export fault, and the suffix makes the hole visible.
`clips_truncated_uploaded_total` counts those saves. An existing truncated key
counts as covering its window (Frigate's export of that window does not get
better by retrying it every pass), and the watermark parses the suffix, so the
archive advances past it normally.

A different fault closes a window without any footage, and Frigate signals it
in two ways. The clip endpoint may answer HTTP 400 with `No recordings found
for the specified time range` - its database no longer lists the window. Or it
may answer HTTP 200 with an empty body: the headers are committed before
ffmpeg runs, so an export that dies before emitting its first packet (the
files the database still lists cannot be opened) arrives as a successful
response with no content. Both mean the recordings are gone from disk, so no
retry can export them. The window is retried like a truncated export - which
covers the race where Frigate drops the segments while the export is running,
and a transient ffmpeg death that a later attempt recovers as a normal clip -
and when every attempt repeats an unavailable answer, an empty `*-no-recordings`
object closes it. The object carries no `.mp4`
extension, because it holds no media and must not be picked up by tooling that
selects the archive by extension. `clips_unavailable_uploaded_total` counts
those markers, and like a truncated clip a marker counts as covering its
window, so the watermark advances. Any other 4xx, a 5xx, a timeout or a
transport cut is a service fault: recordings for those windows are intact, so
they are never marked and the watermark stays pinned until they succeed.

A closing object requires the *whole* chain of attempts to answer with one
fault. A chain that mixes them - a truncated body followed by the
no-recordings answer, or the reverse - proves neither fault is permanent, so
the archiver writes neither `-truncated.mp4` nor a marker: the window fails
as an error and is retried with a fresh chain on a later pass. The same rule
covers the proxy case: a first attempt truncated by a dying Frigate followed
by 503s from the proxy during its restart ends the chain at the first 503
(a service fault is never retried at this level) and archives nothing.

