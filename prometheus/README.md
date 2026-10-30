# Alert rules

`alerts-k8s.yml` is a production ruleset for a Kubernetes deployment
(multiple scrape targets, service discovery, a global job filter in front
of the rules). `vmrule.yaml` is the same ruleset packaged as a
VictoriaMetrics `VMRule` CRD, generated from it by `gen_vmrule.py`.

## Scrape and evaluation intervals

Scrape the exporter every **1 minute** and evaluate rules every
**1 minute**:

```yaml
global:
  evaluation_interval: 1m
scrape_configs:
  - job_name: frigate-motion-exporter
    scrape_interval: 1m
```

With the Prometheus Operator, set `scrapeInterval: 1m` on the ServiceMonitor
and `evaluation_interval: 1m` in the Prometheus resource `spec`.

Why these two numbers:

* The exporter's own loop sleeps `IDLE_SLEEP_SECONDS = 60` between passes,
  so camera gauges and counters change at most about once a minute.
  Scraping faster adds no information; scraping slower loses pass
  boundaries.
* The shortest `for` in the ruleset is 3 minutes (`3m` for `...Down` and
  `...Unscrapeable`); one-minute scrapes put at least three samples inside
  it, so the worst-case detection latency of the fastest rule is ~4
  minutes. Longer scrape intervals push that latency past the `for` itself
  and make `up == 0` detection a coin flip.
* Every `for` value in the ruleset (3, 5, 10, 15, 20, 30, 45, 180, 360
  minutes) is a whole number of one-minute evaluations, and
  `alerts-k8s_test.yml` is written
  and validated at exactly `interval: 1m` / `evaluation_interval: 1m` -
  the timings the scenario headers claim hold only at this cadence.
  Evaluating faster is harmless (alerts can only fire earlier); evaluating
  slower shifts every detection by a multiple of the `for`.
* Range windows (15 m, 30 m, 45 m, 1 h) are at least 15 scrape samples wide
  at this interval, so `delta()`/`increase()` extrapolation cannot shift a
  threshold by a whole pass.

Detection latencies at 1 m / 1 m, for reference:

| Alert | Window + for | Fires after |
|---|---|---|
| FrigateMotionExporterDown | 3 m | ~4 min |
| FrigateMotionExporterUnscrapeable | 3 m lookback + 3 m | ~4 min of total absence (the lookback matches the `for`, so a target that stops reporting fires as soon as its newest sample leaves the 3 m window) |
| NoFrigateQueries | 15 m + 5 m | ~20 min |
| ArchiveTaskWedgedOrDead | 15 m + 10 m | ~25 min |
| ArchiveStalledWithMotion | 30 m + 20 m | ~50 min |
| WatermarkMovedBackwards | 15 m, no `for` | ~1 min after the step; self-resolves 15 min after it |
| ClipsArchivedTruncated | 1 h | ~1 min after the first truncation; stays firing for an hour |
| ArchiveLagHigh | 6 h + 30 m | ~6.5 h behind real time |
| RetentionFloorExceedsSizeTarget | 15 m | ~15 min |
| RetentionSeesForeignArchive | 30 m | ~30 min |
| RetentionCannotReachSizeTarget | 3 h | ~3 h |
| RetentionPassStuck | 5 m + 45 m | ~45 min into a wedged pass |
| RetentionNotPruning | 3 h guard + 15 m | 196 min after the last successful pass: the strict `> 3h` first holds at 181 m, the `for` adds 15 |
| RetentionRefusingToDeleteForHeadroom | 6 h | ~6 h after the growth-reservation guard starts refusing every pass (eight default-length passes); one refusal is the guard working, only the streak is the alert |
| RetentionDeletesRefusedWhileGrowing | two 90 m windows + 3 h delta, no `for` | ~3 h after deletion starts being refused, once two consecutive pass windows refused and the census grew; needs a growing archive - refusals on an under-target bucket are deliberately silent |
| RetentionDeletionEnabled | 6 h | ~6 h after deletion is enabled - info-level, a record rather than a page |

Two boundary rules, both measured rather than reasoned about: a `for` counts
from the first sample, so a rule true from the very first evaluation fires
*exactly* at its `for` (15 m, 30 m and 3 h below are the first firing minutes,
not the last silent ones), and a threshold expressed as a guard inside the
expression is crossed by the guard before the `for` starts counting. Scenario
R-S7 is why the retention rules use a short window plus a `for` instead of a
long window: a partially filled window is evaluated over the samples it has, so
`min_over_time(x[45m]) == 1` is true one minute into a healthy pass.

## Tests

`alerts-k8s_test.yml` holds one scenario per assertion group (crash-loop with
kubelet backoff, S3 blackhole wedge, quiet camera, catch-up pass, clock
skew, prefix switch, retention disabled / dry run / wedged pass / foreign
archive / stopped pruner, ...) - the generator is the list's source of truth,
so its count is not repeated here:

```sh
promtool test rules prometheus/alerts-k8s_test.yml
```

It is generated from `gen_alerts_k8s_tests.py` (same directory); edit the
generator and regenerate rather than editing the YAML by hand.

## Thresholds to tune per deployment

`ExporterResourceHigh` (`process_open_fds > 100` or
`process_resident_memory_bytes > 128e6`) carries this deployment's memory
budget, not a property of the exporter: resident memory scales with the number
of cameras uploading concurrently (~17 MiB per camera at image defaults,
~8-9 MiB with `MALLOC_ARENA_MAX=1`; see *Memory and resource sizing* in
README.md). 128 MB is sized for this instance's small camera set; at 8 cameras
the measured working set reaches ~205 MB while archiving, so the same constant
would fire on a healthy larger deployment. Before reusing the rule, size the
threshold from your own camera count and part size (the README's peak-RSS table
is the starting point) rather than adopting 128 MB; the same applies to the
100-fd half, which assumes a small static camera set.

`RetentionNotPruning` hardcodes 3 hours of staleness, which is four default
prune intervals (`RETENTION_PRUNE_INTERVAL_SECONDS = 2700`). A deployment that
sets the interval above ~1 h has to widen the guard with it, or the rule pages
on a pruner that is running exactly as configured. The same coupling runs the
other way for `RetentionCannotReachSizeTarget` (3 h): with a long interval, a
few *healthy* bounded passes on an over-full bucket can look like a sustained
miss.

The two failure-streak rules are coupled to the same constant in the first
direction. `RetentionDeletesRefusedWhileGrowing` looks at 90-minute windows:
that is two default intervals, the shortest span in which "this pass was
refused" can be distinguished from "passes are being refused" - with a prune
interval above 45 m, a single pass no longer lands inside one window and the
rule goes blind rather than wrong. `RetentionRefusingToDeleteForHeadroom`
(6 h) is eight default passes, the streak that separates the guard doing its
job from a headroom estimate that can never drop; at a longer interval, count
passes, not hours, and widen accordingly. Neither rule false-pages when the
interval shrinks - more passes in the window only makes the streak evidence
stronger.

Retention rules are also the only ones here that read process-global gauges
rather than per-camera series: a prune pass describes the whole archive, so
`retention_*` and `prune_*` carry no `camera` label and one pod's pass says
nothing about another's. Multi-instance deployments (one exporter per bucket is
the documented shape, so more than one pod means more than one archive) should
alert per `instance`, which is what the rules already match on.
