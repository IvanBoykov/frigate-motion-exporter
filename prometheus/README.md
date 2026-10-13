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
* Every `for` value in the ruleset (3, 5, 10, 20, 30 minutes) is a whole
  number of one-minute evaluations, and `alerts-k8s_test.yml` is written
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

## Tests

`alerts-k8s_test.yml` holds 18 failure/health scenarios (crash-loop with
kubelet backoff, S3 blackhole wedge, quiet camera, catch-up pass, clock
skew, prefix switch, ...):

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
