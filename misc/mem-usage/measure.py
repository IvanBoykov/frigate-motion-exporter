"""Run probe_full.py once under a chosen allocator environment, one TSV row out.

CPU cannot be measured from inside the measured process (getrusage(RUSAGE_SELF)
includes the sampler thread), so the probe always runs as a child and CPU comes
from RUSAGE_CHILDREN of this wrapper - minus the out-of-process fake services,
which are descendants too and whose cost an allocator setting reprices as well
(the probe reports their utime/stime/minflt for the subtraction).

usage: measure.py LABEL [VAR=VAL ...] -- [probe_full args]
  LABEL            row label
  VAR=VAL          environment for the child (absent means glibc default)
  probe_full args  clip_mib seconds speed cameras [throttle_ms]

Emits TSV: label cams peak_mib idle_mib cpu_total cpu_user cpu_sys minflt objects conc integrity wall malloc_env
"""
import os
import re
import resource
import subprocess
import sys
import time

def main():
    argv = sys.argv[1:]
    dash = argv.index("--")
    label, env_pairs, probe_args = argv[0], argv[1:dash], argv[dash + 1:]
    env = os.environ.copy()
    for var in ("MALLOC_ARENA_MAX", "MALLOC_TRIM_THRESHOLD_",
                "MALLOC_MMAP_THRESHOLD_"):
        env.pop(var, None)
    for pair in env_pairs:
        k, _, v = pair.partition("=")
        env[k] = v

    t0 = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      "probe_full.py")] + probe_args,
        capture_output=True, text=True, env=env)
    out = proc.stdout
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall = time.perf_counter() - t0

    def grab(pattern):
        m = re.search(pattern, out)
        return m.group(1) if m else "?"

    # Echo what the child actually ran with: a label is not an environment, and
    # a row whose variables were silently forgotten is a duplicate of `default`
    # that looks like a measurement.
    effective = ",".join(
        f"{k.removeprefix('MALLOC_').lower()}={env[k]}"
        for k in ("MALLOC_ARENA_MAX", "MALLOC_TRIM_THRESHOLD_",
                  "MALLOC_MMAP_THRESHOLD_") if k in env) or "."

    cams = grab(r"cams=(\d+)")
    peak = grab(r"RSS peak during run\s*:\s*([\d.]+)")
    idle = grab(r"RSS at end of run\s*:\s*([\d.]+)")
    conc = grab(r"peak concurrent uploads\s*:\s*(\d+)")
    objs = grab(r"objects written\s*:\s*(\d+)")
    # RUSAGE_CHILDREN folds in every waited-for descendant, so with the
    # fakes out-of-process their CPU and faults must come back out: they
    # scale with volume, but allocator settings reprice the fake's hashing
    # and chunking too, and that cost is not the uploader's.
    fake_cpu = float(grab(r"fake cpu excluded\s*:\s*([\d.]+)"))
    fake_sys = float(grab(r"fake sys excluded\s*:\s*([\d.]+)"))
    fake_minflt = int(grab(r"fake minflt excluded\s*:\s*(\d+)") or 0)
    integrity = "OK" if "OK - every object" in out else "FAIL"
    if proc.returncode != 0:
        print(f"{label}\t{cams}\tERROR rc={proc.returncode}\t"
              f"{proc.stderr[-300:]}", file=sys.stderr)
    print(f"{label}\t{cams}\t{peak}\t{idle}\t"
          f"{max(r.ru_utime + r.ru_stime - fake_cpu, 0):.1f}\t"
          f"{max(r.ru_utime - fake_cpu + fake_sys, 0):.1f}\t"
          f"{max(r.ru_stime - fake_sys, 0):.1f}\t"
          f"{max(r.ru_minflt - fake_minflt, 0)}\t"
          f"{objs}\t{conc}\t{integrity}\t{wall:.0f}s\t{effective}")

main()
