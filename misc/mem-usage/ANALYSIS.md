# What the allocator settings actually do

Source: `results.tsv`, produced with the harness in this directory on 2026-10-12:
glibc 2.36 (Debian 12), Python 3.11, 16 visible CPUs / 8 by affinity, fake
services out-of-process. Every row archived 150 clips of 50 MiB (~7.3 GiB) with
concurrency verified server-side (`conc` equals `cams` everywhere) and passed the
byte-identity check. Because the volume is fixed, CPU and wall time are
comparable across rows.

An earlier version of this file recorded a matrix from the previous harness
(`results-20261011-inprocess.tsv`, glibc 2.41, fakes inside the measured
process, archiver code from before the part-release fix). Two of its findings
were harness artifacts, and all of its absolute numbers measured a code path
that held the previous part alive across every part transition. The durable
conclusions survive the redo; where the new numbers contradict the old text,
the contradiction is called out below. The archive file is kept as provenance,
not as evidence.

Configuration labels: `A1`/`A2` = `MALLOC_ARENA_MAX=1/2`, `T` =
`MALLOC_TRIM_THRESHOLD_=1MiB`, `MMAP4M`/`MMAP6M` =
`MALLOC_MMAP_THRESHOLD_=4/6 MiB`, `M0` = `MALLOC_MMAP_MAX_=0`,
`M6` = `MALLOC_MMAP_THRESHOLD_=6MiB`. `default` sets nothing. `t`-prefixed rows
throttle the fake S3 3 ms per 64 KiB read to emulate a network-bound endpoint.
`OLD_*`/`NEW_*` rows are an A/B of the archiver code itself on the same
harness, with `git stash` as the only change.

## The code fix first: release the part you just uploaded

The A/B (`OLD_*` vs `NEW_*`, identical harness, volume and code path except
for one `del part_data` after each `upload_part`):

| Config (4 cams) | peak before | peak after | CPU before | CPU after |
| --- | --- | --- | --- | --- |
| default | 170.2 | 143.7 (-16 %) | 31.1 s | 31.2 s |
| A1 | 128.2 | 114.5 (-11 %) | 27.3 s | 27.7 s |

`part_data = buffer[:part_size]` allocates the new slice *before* the name drops
the old part, so every part transition held two part copies live at once. The
`del` after the PUT returns removes the overlap: the isolated live-peak probe
(`probe_part.py`, tracemalloc) drops from 17.6 to 12.6 MiB for a 50 MiB clip at
5 MiB parts, and the RSS peak drops 11-16 % with CPU flat to noise. Everything
below is measured on the fixed code.

## Memory

Peak RSS in MiB, with the change against `default` at the same camera count:

| Config | 2 cams | 4 cams | 8 cams | 16 cams |
| --- | --- | --- | --- | --- |
| default | 104.7 | 139.6 | 204.9 | 314.1 |
| A2 | 92.4 (-12 %) | 116.3 (-17 %) | 153.4 (-25 %) | - |
| A1 | 86.7 (-17 %) | 114.2 (-18 %) | 142.4 (-31 %) | 199.2 (-37 %) |
| MMAP4M | 78.3 (-25 %) | 94.8 (-32 %) | 132.1 (-36 %) | - |
| A1+MMAP4M | 79.4 (-24 %) | 96.7 (-31 %) | 125.1 (-39 %) | - |
| A1+T | 74.2 (-29 %) | 84.7 (-39 %) | 112.0 (-45 %) | - |
| A1+T+M6 | - | 112.0 (-20 %) | 142.9 (-30 %) | - |
| M0 | 126.9 (4c, -9 %) | - | 193.1 (8c, -6 %) | - |

Repeats agree within ~1.5 MiB of peak and ~2.5 s of CPU (`A1_8` vs `A1_8r`),
which is the noise floor of the series. The per-camera slope of the peak is
~17 MiB at image defaults and ~9 MiB with `A1`; the peak is flat in clip
length (streaming + multipart, a longer clip costs time, never memory).

Idle RSS (after the pass, what the process keeps until restart) still splits
the settings into two families, exactly as the old series found: anything with
`T`, and `MMAP4M` on its own, lands at 59-71 MiB regardless of cameras
(`A1+MMAP4M` is close, 62-89 MiB). `A1` alone holds
63-196 MiB and grows with cameras - trimming is what returns pages to the OS -
and at 8 cameras its idle RSS (111.6 MiB) is again *above* the default's
(109.6 MiB), while at 16 cameras it barely falls below its own peak (196.2
idle vs 199.2 peak). The arena cap lowers the peak a container limit must
clear; it does not leave the process lighter between passes.

`MALLOC_MMAP_MAX_=0` (disable mmap for big allocations entirely) is not worth
recommending: it buys -6..-9 % peak for +19..+25 % CPU and doubled system
time, and on top of `A1` it buys nothing - it ties `A1` at 4 cameras (114.4 vs
114.2) and is a few MiB *worse* at 8 (147.9 vs 142.4/143.7). The mmap traffic it removes was never the cost centre; the
*page churn* of re-mmapping every part is, and `M0` only attacks the descriptor
side of it.

## CPU and throughput

Total CPU seconds and wall time per identical 150-clip volume; CPU is the
uploader's own - the fake's utime/stime/minflt are reported by the fake and
subtracted (`fake cpu excluded` lines):

| Config | 2 cams CPU / wall | 4 cams CPU / wall | 8 cams CPU / wall |
| --- | --- | --- | --- |
| default | 33.9 / 36 s | 33.8 / 30 s | 38.0 / 33 s |
| A2 | 31.3 / 33 s | 31.8 / 24 s | 36.1 / 24 s |
| A1 | 26.3 / 28 s | 28.7 / 21 s | 33.4 / 21 s |
| MMAP4M | 45.4 / 44 s | 52.6 / 44 s | 56.2 / 47 s |
| A1+MMAP4M | 38.4 / 37 s | 44.9 / 35 s | 45.6 / 33 s |
| A1+T | 63.5 / 74 s | 68.4 / 58 s | 71.5 / 59 s |
| A1+T+M6 | - | 33.4 / 24 s | 39.6 / 27 s |
| M0 | - | 42.1 / 39 s | 45.1 / 38 s |
| A1+M0 | - | 30.2 / 23 s | 36.7 / 25 s |

At 16 cameras: default 45.5 s / 34 s, `A1` 37.1 s / 23 s, `A1+MMAP4M` 47.4 s /
33 s. Against the throttled (network-bound) fake: default 39.1 s / 117 s, `A1`
31.5 s / 110 s, `A1+T` 62.3 s / 127 s - `A1` is again *faster*, and the trim
penalty collapses to +8.5 % wall because the run is waiting on the fake, not on
page faults.

## What is actually being paid for

- **`MALLOC_ARENA_MAX=1` is a free win here.** Peak falls 17 % at 2 cameras and
  31-37 % at 8-16, total CPU falls 12-22 %, and wall time improves (21 s vs
  33 s at 8 cameras). Minor faults drop about 10x (0.3 M vs 3.0 M at 8
  cameras): fewer arenas means the pages of a finished part get reused by the
  next thread instead of being stranded in a private heap. The standard
  objection - one arena serialises allocators on the heap lock - again did not
  appear at upload concurrency up to 16; these threads spend their allocation
  time inside `memcpy` of a part under the GIL anyway. Caveat unchanged: the
  workload is one large memcpy per thread; genuinely allocation-parallel
  threads could contend harder.
- **The arena count stops mattering once something else pins the pages.**
  That was the old series' direct reading (`A1`/`A2`/`A4` peaks agreeing within
  4 % with `T` set); this matrix confirms the other half - without `T` the
  count is the whole effect (114.2 vs 116.3 vs 139.6 at 4 cameras for
  A1/A2/default; closer than the old series' 127.6 / 156.9 / 173.0 because the
  part-release fix already removed the biggest per-transition allocation). The
  old correction stands: rows whose environment variable was silently not
  passed read as duplicates of `default`, which is why every row now echoes
  its actual `malloc_env`.
- **The whole cost of `T` is the mmap threshold it pins, now proven directly
  instead of inferred.** `MALLOC_TRIM_THRESHOLD_` has no separate tunable knob;
  setting it also pins the dynamic mmap threshold to its value, so every 5 MiB
  part becomes a fresh `mmap` + page faults + `munmap`. Explicitly re-raising
  the threshold above the part size with `MMAP6M` while keeping the trim
  (`A1+T+M6`) collapses system time from 46.5 s to 11.4 s at 4 cameras - total
  CPU returns to the default's 33 s - and the peak stays at `A1`'s level
  (112.0 vs 114.2). Idle RSS is mixed: slightly worse than `A1` at 4 cameras
  (85.9 vs 74.4), better at 8 (99.0 vs 111.6). The old series inferred this from a
  matching faults pattern; this is the mechanism under direct control. What it
  means practically: the trim threshold itself costs ~nothing, and every
  expensive `T` measurement is really a *mmap churn* measurement.
- **Even when the churn is real, it is cheaper than the old series said.**
  `A1+T` now measures 1.87-2.02x CPU and 1.79-2.06x wall (was 2.1-2.6x /
  2.6-3.8x), and `MMAP4M` alone 1.34-1.56x CPU (was 1.74-1.99x). The old
  multipliers were partly an artifact: the in-process fake hashed every byte
  under the *same* allocator environment, so an allocator that slows mmap also
  slowed the meter. Out-of-process, the fake's cost is reported and subtracted,
  and the environment moves only the uploader.
- **`MALLOC_MMAP_THRESHOLD_` still has to be placed relative to `PART_SIZE`.**
  Below the part (`MMAP4M`) it forces every part through `mmap`: peak -25..-36
  %, close to what `T` buys, at a now-honest ~1.4-1.6x CPU. Pairing it with
  `A1` gives back most of that (1.13-1.33x) for ~1-5 MiB more peak -
  `A1+MMAP4M` remains the best memory-per-CPU point in the matrix. Above the
  part (`MMAP6M` on top of `T`) it un-pins the churn, as above. A threshold
  below `PART_SIZE` only works while `PART_SIZE` is stable; if the part size
  ever drops below the threshold, the setting silently stops doing anything.
- **`MALLOC_MMAP_MAX_=0` removes the churn by forbidding mmap instead of
  re-placing the threshold, and pays for it** (see Memory): -6..-9 % peak,
  +19..+25 % CPU, and no gain on top of `A1` - a tie at 4 cameras and a few
  MiB worse at 8. The knob to touch is the threshold, not the count.

## Bottom line

Measured on the fixed code and fixed harness (new baseline = `default` rows):

| Setting | Peak vs default | CPU vs default | Wall vs default |
| --- | --- | --- | --- |
| `MALLOC_ARENA_MAX=1` | -17..-37 % | 0.78-0.88x | 0.64-0.78x |
| `MALLOC_ARENA_MAX=2` | -12..-25 % | 0.92-0.95x | 0.73-0.92x |
| `MALLOC_MMAP_THRESHOLD_=4MiB` | -25..-36 % | 1.34-1.56x | 1.22-1.47x |
| `A1` + `MALLOC_MMAP_THRESHOLD_=4MiB` | -24..-39 % | 1.13-1.33x | 1.00-1.17x |
| `MALLOC_TRIM_THRESHOLD_=1MiB` + `A1` | -29..-45 % | 1.87-2.02x | 1.79-2.06x |

`MALLOC_ARENA_MAX=1` is still the setting to reach for first: it costs nothing
measurable and improved CPU and throughput here. It does not return memory
between passes, so pair it with the trim threshold only when idle RSS or the
last 10 % of peak reduction actually matters, and know that the trim's price is
mmap churn (raising `MALLOC_MMAP_THRESHOLD_` above `PART_SIZE` together with it
buys the CPU back at the cost of most of the peak reduction).
`A1+MALLOC_MMAP_THRESHOLD_=4MiB` sits between them: -31..-39 % peak for
~1.2-1.3x CPU. The image sets none of them; a slower archive delays every
alert the exporter exists to send.

## A ceiling that is not the allocator

Peak RSS stops scaling once concurrent uploads hit the `asyncio.to_thread`
default pool, whose size depends on the interpreter version:
`min(32, os.cpu_count() + 4)` on Python <= 3.12 (`concurrent.futures.thread`,
which `to_thread` delegates to) and `min(32, os.process_cpu_count() + 4)` on
3.13+. The difference matters exactly where this matters operationally:
`os.cpu_count()` reports the machine's CPUs while
`process_cpu_count()` follows the process's CPU *affinity*, so the same
container gets a pool of 20 on 3.11 and 12 on 3.14 (the image's Python) on the
box these numbers came from - 16 visible CPUs, 8 by affinity. Measured on 3.11:
16 cameras produced `peak concurrent uploads = 16` (pool 20); the old series
saw 12 on an 8-CPU host. Size memory from the *uploaded-concurrently* number,
which the harness prints, not from the camera count, and remember the pool
follows a cpuset only on 3.13+; a pod throttled by CFS *quota* never shrinks
its pool, it just goes slower.
