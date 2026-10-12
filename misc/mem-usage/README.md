# Memory / CPU harness

Measures what the archiver process actually holds (RSS) and burns (CPU) while
uploading, under allocator settings and camera counts, without touching a real
Frigate or S3. `fake_services.py` runs a fake Frigate and a fake S3 on loopback
in a *separate process*; `probe_full.py` drives the real pipeline
(`config.load_config` -> `_run_cameras` -> `run_camera` -> upload) against them
and samples RSS at 1 ms; `measure.py` runs one probe as a child under a chosen
allocator environment and emits a single TSV row with RSS *and* CPU. CPU cannot
come from the measured process's own `getrusage` (it would include the sampler
thread), so it comes from the parent's `RUSAGE_CHILDREN` - minus the fake
services' own `utime`/`stime`/`minflt`, which the parent also waits for and
which the fake reports over its admin endpoint.

## Invariants - break these and the numbers are meaningless

- The fakes are out-of-process. An in-process fake shares `/proc/self` with the
  archiver (its payload generation, body hashing and socket buffers land in the
  measured RSS) *and* shares its allocator environment (`MALLOC_*` is
  per-process, so a setting under test repriced the fake's hashing too - the
  old "trim costs 2.6x CPU" partly measured the meter being slowed down). The
  probe subtracts the fake's CPU and faults and prints what it subtracted;
  `--inproc` keeps the old mode only for A/B-ing the harness itself.
- `/api/config` is built from the requested camera count. A fixed config
  silently runs two cameras no matter what the run is labelled; a previous
  round of "N camera" measurements was actually two cameras everywhere.
- Concurrency is counted, not assumed: the fake S3 tracks uploads in flight
  from first part to Complete, and `peak concurrent uploads` must equal the
  camera count in the row, or the run did not test what it claims.
- The fake S3 streams request bodies into an incremental digest 64 KiB at a
  time; buffering them would charge server-side heap to the measured RSS.
- Byte-identity: the payload is position-tagged, every object's digest must
  match the clip stream (`ok=OK` in the row), so part-boundary bugs cannot
  hide behind a flat profile.
- CPU across configurations is only comparable when upload volume is fixed:
  pass a `clip_target` (last probe argument) so every run ends after the same
  number of completed clips, and check the `objects` column matches.
- Every row echoes the allocator variables the child actually ran with
  (`malloc_env`; `.` for none), because a label is not an environment: a row
  whose variables were forgotten is a duplicate of `default` wearing a
  costumed name - that is how the arena count once got declared irrelevant.
  `MALLOC_*` variables are read at process startup, so they must be set on the
  child's environment, which `measure.py` handles.

## Running

From the repository root:

    python3 misc/mem-usage/measure.py LABEL [VAR=VAL ...] -- \
        clip_mib seconds speed cameras throttle_ms segments_per_hour clip_target

Example - arena cap of 2, 50 MiB clips, 4 cameras, 150 clips of volume:

    python3 misc/mem-usage/measure.py A2+T MALLOC_ARENA_MAX=2 \
        MALLOC_TRIM_THRESHOLD_=1048576 -- 50 240 60 4 0 60 150

Row columns: `label cams peak_rss_mib idle_rss_mib cpu_total cpu_user cpu_sys
minflt objects conc integrity wall malloc_env`. RSS is read from
`/proc/self/status` of the probe; the fake's RSS is sampled slowly (every poll
is an HTTP request charged to the measured process) and reported, never mixed
in.

`probe_malloc.py` isolates allocator behaviour from HTTP (replays the uploader's
allocation pattern in N threads), `probe_part.py` measures the live peak of a
single upload with tracemalloc, and `probe_real.py` runs the pipeline against
the real Frigate from `.secrets` while still writing to the fake S3 - it needs
`FRIGATE_USER` and `FRIGATE_PASSWORD` in the environment and prints nothing
secret.

## Results

`results.tsv` holds the current matrix (harness v2, 2026-10-12: 6 allocator
configurations x 2/4/8 cameras plus 16-camera, throttled, code-A/B and extra
knob rows; 50 MiB clips, 150 clips per run, glibc 2.36, Python 3.11) and
`ANALYSIS.md` the conclusions drawn from it. `results-20261011-inprocess.tsv`
is the archived previous series (in-process fakes, pre-part-release code,
glibc 2.41): kept as provenance, not as evidence - two of its conclusions were
harness artifacts and its absolute numbers measured a code path that kept the
previous part alive across every part transition. The README at the repository
root carries only the summary and links here.
