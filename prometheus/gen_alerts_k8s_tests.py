"""Generate prometheus/alerts-k8s_test.yml from failure scenarios.

Series mirror only states run_camera/supervise_camera can produce (src/):
  - camera_task_running: 1 right after the watermark read returns, 0 while the
    supervisor backs off; never 1 without the watermark gauge existing.
  - camera_watermark_timestamp_seconds: set after the read, then per storing
    pass; absent only before the first read or with the process.
  - camera_motion_intervals_found: set per pass before uploading; keeps its
    last value while a pass is stuck in upload.
  - frigate_response_seconds_count{kind="json"}: one observation per camera per
    pass including failed calls; frozen for the whole duration of an upload.
"""
import re

RULES = "prometheus/alerts-k8s.yml"
OUT = "prometheus/alerts-k8s_test.yml"
T0 = 1_700_000_000
CAM, YARD = "dacha_cam", "yard_cam"
INST_A, INST_B = "10.128.0.18:9108", "10.128.0.19:9108"


def s(name, values, camera=CAM, instance=INST_A, **extra):
    labels = {}
    if camera is not None:
        labels["camera"] = camera
    if instance is not None:
        labels["instance"] = instance
    labels.update(extra)
    inner = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f'      - series: {name}{{{inner}}}\n        values: "{values}"'


def healthy(n=720, cams=(CAM,), restart_at=None, inst=INST_A):
    """One storing pass per minute, watermark = now-120 s.

    restart_at: the camera task dies at that minute (an exception inside
    run_camera), the supervisor backs off 5s+10s (~2 samples at 1m cadence,
    failures 1..2 per camera_restart_delay(5,300)), then the new task re-lists
    S3: the watermark regresses ~8 min (clip-start re-alignment, under the
    10-min interval chunk), catches up at 2x, then tracks again. The process
    itself never dies: one process_start change.
    """
    lines = [s("up", f"1x{n}", camera=None, instance=inst)]
    for c in cams:
        if restart_at is None:
            lines.append(s("camera_task_running", f"1x{n}", camera=c, instance=inst))
            lines.append(s("camera_motion_intervals_found", f"0x{n}", camera=c,
                           instance=inst))
            lines.append(s("camera_consecutive_failures", f"0x{n}", camera=c,
                           instance=inst))
            lines.append(s("camera_watermark_timestamp_seconds",
                           f"-120+60x{n-1}", camera=c, instance=inst))
        else:
            r = restart_at
            pre = f"-120+60x{r-1}"
            drop = -120 + 60 * (r - 1) - 480
            post = f"{drop}+120x2"
            back = drop + 240
            lines.append(s("camera_task_running",
                           f"1x{r} 0x2 1x{n-r-2}", camera=c, instance=inst))
            lines.append(s("camera_motion_intervals_found", f"0x{n}", camera=c,
                           instance=inst))
            lines.append(s("camera_consecutive_failures",
                           f"0x{r} 1 2 0x{n-r-2}", camera=c, instance=inst))
            lines.append(s("camera_watermark_timestamp_seconds",
                           f"{pre} {post} {back}+60x{n-r-4}", camera=c,
                           instance=inst))
        lines.append(s("clips_uploaded_total", f"0+1x{n}", camera=c, instance=inst))
    if restart_at is None:
        ps = f"-43200x{n}"
    else:
        ps = f"-43200x{restart_at} {restart_at}x{n-restart_at}"
    lines.append(s("process_start_time_seconds", ps, camera=None, instance=inst))
    lines.append(s("process_open_fds", f"12x{n}", camera=None, instance=inst))
    lines.append(s("process_resident_memory_bytes", f"96075776x{n}", camera=None,
                   instance=inst))
    lines.append(s("frigate_response_seconds_count", f"0+{len(cams)}x{n}",
                   camera=None, instance=inst, kind="json"))
    lines.append(s("frigate_response_seconds_count", f"0+{len(cams)}x{n}",
                   camera=None, instance=inst, kind="clip_ttfb"))
    return lines


def replace_series(lines, name, new_line):
    """Drop name's series line and its values line, then append new_line."""
    out, skip = [], False
    for l in lines:
        if f"series: {name}{{" in l:
            skip = True
            continue
        if skip:
            skip = False
            continue
        out.append(l)
    out.append(new_line)
    return out


def load_annotations():
    import yaml
    sev, ann = {}, {}
    for group in yaml.safe_load(open(RULES))["groups"]:
        for rule in group.get("rules") or []:
            name = rule["alert"]
            sev[name] = rule["labels"]["severity"]
            ann[name] = (rule["annotations"]["summary"],
                         rule["annotations"]["description"])
    return sev, ann


def render(text, labels):
    text = re.sub(r"\{\{\s*\$labels\.(\w+)\s*\}\}",
                  lambda m: str(labels.get(m.group(1), "?")), text)
    return text.replace('"', '\\"')


def checks(times, alertnames, fired):
    return [(t, a, fired.get((t, a), [])) for t in times for a in alertnames]


# Which labels survive each rule's expression: absent() carries none, `up`
# carries instance, counters carry kind, gauge expressions carry camera+instance.
RULE_LABELS = {
    "FrigateMotionExporterDown": lambda cam, inst: {"instance": inst},
    "FrigateMotionExporterUnscrapeable": lambda cam, inst: {},
    "ArchiveTaskWedgedOrDead": lambda cam, inst: {"camera": cam, "instance": inst},
    "ArchiveStalledWithMotion": lambda cam, inst: {"camera": cam, "instance": inst},
    "ArchiveLagHigh": lambda cam, inst: {"camera": cam, "instance": inst},
    "FootageLostInFrigate": lambda cam, inst: {"camera": cam, "instance": inst},
    "PermanentConfigRejection": lambda cam, inst: {"camera": cam, "instance": inst,
                                                   "kind": "s3_accessdenied"},
    "NoFrigateQueries": lambda cam, inst: {"kind": "json", "instance": inst},
    "ProcessRestartLoop": lambda cam, inst: {"instance": inst},
    "ClipsArchivedTruncated": lambda cam, inst: {"camera": cam, "instance": inst},
    "WatermarkMovedBackwards": lambda cam, inst: {"camera": cam, "instance": inst},
    "WatermarkInFuture": lambda cam, inst: {"camera": cam, "instance": inst},
    "CameraInDeepBackoff": lambda cam, inst: {"camera": cam, "instance": inst},
    "UnclassifiedErrorKind": lambda cam, inst: {"camera": cam, "instance": inst,
                                                "kind": "unknown"},
    "ExporterResourceHigh": lambda cam, inst: {"instance": inst},
}

ALL = ["FrigateMotionExporterDown", "FrigateMotionExporterUnscrapeable",
       "ArchiveTaskWedgedOrDead", "ArchiveStalledWithMotion", "ArchiveLagHigh",
       "FootageLostInFrigate", "PermanentConfigRejection", "NoFrigateQueries",
       "ProcessRestartLoop", "ClipsArchivedTruncated", "WatermarkMovedBackwards",
       "CameraInDeepBackoff", "UnclassifiedErrorKind", "ExporterResourceHigh",
       "WatermarkInFuture"]


def build():
    sc = []

    sc.append(("S1 healthy 12 h, 2 cameras, pod restart at +3 h (4 min backoff, "
               "8 min safe watermark regression, one process start): no rule "
               "fires, including the minutes right around the restart.",
               healthy(720, cams=(CAM, YARD), restart_at=180),
               checks(["119m", "182m", "186m", "190m", "240m", "719m"], ALL, {})))

    fired = {("3m", "FrigateMotionExporterUnscrapeable"): [({}, None)],
             ("6m", "FrigateMotionExporterUnscrapeable"): [({}, None)]}
    sc.append(("S2 pod deleted/never scheduled: no target, hence no up series. "
               "Unscrapeable fires; up==0 stays silent - the hole the absent_over_time "
               "arm closes.", [],
               checks(["2m", "3m", "6m"],
                      ["FrigateMotionExporterDown",
                       "FrigateMotionExporterUnscrapeable"], fired)))

    fired = {("5m", "FrigateMotionExporterUnscrapeable"): [({}, None)],
             ("15m", "FrigateMotionExporterDown"): [({}, None)],
             ("15m", "FrigateMotionExporterUnscrapeable"): [({}, None)]}
    sc.append(("S3 target still listed, /metrics stops answering: Down fires, "
               "Unscrapeable fires too (no exporter metric series at all).",
               [s("up", "1x10 0x30", camera=None)],
               checks(["5m", "15m"], ["FrigateMotionExporterDown",
                                      "FrigateMotionExporterUnscrapeable"],
                      fired)))

    lines = [s("up", "1x90", camera=None),
             s("camera_task_running", "0x90"),
             s("camera_consecutive_failures", "0x1 0x89"),
             s("camera_motion_intervals_found", "0x90"),
             s("frigate_response_seconds_count", "0+1x1 1x89", camera=None,
               kind="json"),
             s("process_start_time_seconds", "-43200x90", camera=None),
             s("process_open_fds", "12x90", camera=None),
             s("process_resident_memory_bytes", "96075776x90", camera=None)]
    fired = {("26m", "ArchiveTaskWedgedOrDead"): [({}, None)],
             ("26m", "NoFrigateQueries"): [({}, None)],
             ("60m", "ArchiveTaskWedgedOrDead"): [({}, None)],
             ("60m", "NoFrigateQueries"): [({}, None)]}
    sc.append(("S4 S3 blackhole wedging the watermark read: WedgedOrDead fires "
               "at 24m+, NoFrigateQueries at ~21m (JSON and clip counters both "
               "frozen - nothing is being downloaded either). Stalled and Lag stay silent "
               "BY DESIGN (the watermark series was never exported, nothing to "
               "match) - this pair is the coverage for that state.", lines,
               checks(["26m", "60m"], ["ArchiveTaskWedgedOrDead",
                                       "ArchiveStalledWithMotion", "ArchiveLagHigh",
                                       "NoFrigateQueries"],
                      fired)))

    lines = healthy(120, cams=(YARD,), inst=INST_B)
    lines += [s("up", "1x120", camera=None, instance=INST_A),
              s("camera_task_running", "0x120", instance=INST_A),
              s("process_start_time_seconds", "300x120", camera=None,
                instance=INST_A)]
    fired = {("20m", "ArchiveTaskWedgedOrDead"): [],
             ("35m", "ArchiveTaskWedgedOrDead"): [({"instance": INST_A}, None)]}
    sc.append(("S5 rolling update, two pods: young wedged pod A is guarded by "
               "ITS OWN age, not pod B's (with and on() pod B suppressed A "
               "forever). Silent at 20m (A younger than 15 min), fires at ~30m "
               "on A alone.", lines,
               checks(["20m", "35m"], ["ArchiveTaskWedgedOrDead"], fired)))

    n = 90
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", f"1x{n}"),
             s("camera_motion_intervals_found", f"0x2 5x35 0x{n-37}"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds",
               f"-120+60x2 -60x35 -60+60x{n-38}"),
             s("frigate_response_seconds_count", f"0+1x2 2x35 3+1x{n-37}",
               camera=None, kind="json"),
             s("frigate_response_seconds_count", f"0x2 1+1x34 35+1x{n-36}",
               camera=None, kind="clip_ttfb"),
             s("process_start_time_seconds", f"-43200x{n}", camera=None),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    sc.append(("S6 one long HEALTHY pass: 5 intervals uploaded for 35 min "
               "(watermark frozen mid-pass, JSON counter frozen mid-pass, "
               "motion gauge pinned). Stalled, Lag and NoFrigateQueries all "
               "ride it out - NoFrigateQueries because clip_ttfb grows while "
               "JSON sleeps (catch-up is life).", lines,
               checks(["45m", "60m", "75m"], ["ArchiveStalledWithMotion",
                                              "ArchiveLagHigh",
                                              "NoFrigateQueries"], {})))

    n = 120
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", "1x20 0x100"),
             s("camera_motion_intervals_found", f"4x{n}"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds", f"-120+60x10 420x{n-10}"),
             s("frigate_response_seconds_count", "0+1x1 1x119", camera=None,
               kind="json"),
             s("frigate_response_seconds_count", "0+1x20 20x100", camera=None,
               kind="clip_ttfb"),
             s("process_start_time_seconds", f"-43200x{n}", camera=None),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    fired = {("1h", "ArchiveStalledWithMotion"): [({}, None)],
             ("55m", "NoFrigateQueries"): [({}, None)],
             ("1h", "NoFrigateQueries"): [({}, None)]}
    # clip_ttfb froze at 20m (wedged on S3, remaining clips never fetched):
    # both increases are 0 from 35m, firing from 40m.
    fired[("40m", "NoFrigateQueries")] = [({}, None)]
    sc.append(("S7 real stall: 4 intervals pending, watermark frozen 50+ min, "
               "task wedged uploading: Stalled fires at ~51m, NoFrigateQueries "
               "follows; Lag silent (watermark only 1 h deep, not 6).", lines,
               checks(["40m", "55m", "1h"], ["ArchiveStalledWithMotion",
                                       "ArchiveLagHigh", "NoFrigateQueries"],
                      fired)))

    n = 180
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", f"1x{n}"),
             s("camera_motion_intervals_found", "3x180"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds", f"{-7*3600}x180"),
             s("frigate_response_seconds_count", "0+1x180", camera=None,
               kind="json"),
             s("process_start_time_seconds", "-43200x180", camera=None),
             s("process_open_fds", "12x180", camera=None),
             s("process_resident_memory_bytes", "96075776x180", camera=None)]
    fired = {("20m", "ArchiveLagHigh"): [],
             ("65m", "ArchiveLagHigh"): [({}, None)],
             ("65m", "ArchiveStalledWithMotion"): [({}, None)]}
    sc.append(("S8 7 h deep frozen watermark WITH motion: Lag fires after its "
               "30m `for` (silent at 20m); Stalled co-fires at ~51m.", lines,
               checks(["20m", "65m"], ["ArchiveLagHigh",
                                       "ArchiveStalledWithMotion"], fired)))

    n = 240
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", f"1x{n}"),
             s("camera_motion_intervals_found", "0x240"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds", f"{-8*3600}+60x{n-1}"),
             s("frigate_response_seconds_count", f"0+1x{n}", camera=None,
               kind="json"),
             s("process_start_time_seconds", f"-43200x{n}", camera=None),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    sc.append(("S9 8 h deep watermark, ZERO motion, tracker healthy: the exact "
               "night-quiet case - Lag and Stalled silent.", lines,
               checks(["120m", "239m"], ["ArchiveLagHigh",
                                         "ArchiveStalledWithMotion"], {})))

    lines = healthy(60)
    lines.append(s("clips_unavailable_uploaded_total", "0x30 1x30"))
    lines.append(s("clips_truncated_uploaded_total", "0x30 9x30"))
    fired = {("35m", "FootageLostInFrigate"): [({}, None)],
             ("35m", "ClipsArchivedTruncated"): [({}, None)],
             ("45m", "FootageLostInFrigate"): [],
             ("45m", "ClipsArchivedTruncated"): [({}, None)]}
    sc.append(("S10 unavailable=1 + truncated=9 in one 10-min window: separate "
               "rules fire separately (the old 'or' merged them into one "
               "warning). Loss alert expires after its 10-min window; "
               "truncation holds an hour.", lines,
               checks(["35m", "45m"], ["FootageLostInFrigate",
                                       "ClipsArchivedTruncated"], fired)))

    n = 45
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", "1x4 0x41"),
             s("camera_motion_intervals_found", "0x45"),
             s("camera_consecutive_failures",
               "0x5 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15x29"),
             s("camera_watermark_timestamp_seconds",
               f"-120+60x4 120x41"),
             s("errors_total", "0x5 1+5x40", kind="s3_accessdenied"),
             s("frigate_response_seconds_count", "0+1x4 1x41", camera=None,
               kind="json"),
             s("process_start_time_seconds", f"-43200x{n}", camera=None),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    perm = {"camera": CAM, "instance": INST_A, "kind": "s3_accessdenied"}
    fired = {("12m", "PermanentConfigRejection"): [(perm, None)],
             ("12m", "CameraInDeepBackoff"): [],
             ("12m", "ArchiveTaskWedgedOrDead"): [],
             ("17m", "PermanentConfigRejection"): [(perm, None)],
             ("17m", "CameraInDeepBackoff"): [({}, None)],
             ("17m", "ArchiveTaskWedgedOrDead"): [],
             ("30m", "ArchiveTaskWedgedOrDead"): [({}, None)],
             ("30m", "PermanentConfigRejection"): [(perm, None)],
             ("30m", "CameraInDeepBackoff"): [({}, None)]}
    sc.append(("S11 AccessDenied storm: Permanent fires immediately, "
               "DeepBackoff when failures hold >= 5 for 5 min, WedgedOrDead "
               "only after 15 dead minutes + 10 for. Inhibition (Alertmanager "
               "side) suppresses the latter two under Permanent.", lines,
               checks(["12m", "17m", "30m"], ["PermanentConfigRejection",
                                              "CameraInDeepBackoff",
                                              "ArchiveTaskWedgedOrDead"], fired)))

    out = replace_series(healthy(45), "process_start_time_seconds",
                         s("process_start_time_seconds",
                           "-3600x10 600x10 1200x8 1800x17", camera=None))
    fired = {("10m", "ProcessRestartLoop"): [],
             ("35m", "ProcessRestartLoop"): [({"instance": INST_A}, None)]}
    sc.append(("S12 crash loop: 4 starts in 30 min; RestartLoop silent at 10m "
               "(one change is a deploy, and even a second is tolerated), "
               "fires at 35m.", out,
               checks(["10m", "35m"], ["ProcessRestartLoop"], fired)))

    n = 70
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", f"1x{n}"),
             s("camera_motion_intervals_found", "0x70"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds",
               f"-120+60x30 -10920+60x40"),
             s("frigate_response_seconds_count", f"0+1x{n}", camera=None,
               kind="json"),
             s("process_start_time_seconds", f"-43200x{n}", camera=None),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    fired = {("35m", "WatermarkMovedBackwards"): [({}, None)],
             ("55m", "WatermarkMovedBackwards"): []}
    sc.append(("S13 S3_PREFIX switch: 3 h step down fires MovedBackwards for "
               "as long as the drop is inside the 15 min window, then resolves. "
               "The 8 min restart regression in S1 never fired it.", lines,
               checks(["35m", "55m"], ["WatermarkMovedBackwards"], fired)))

    out = replace_series(healthy(80), "process_open_fds",
                         s("process_open_fds", "150x80", camera=None))
    fired = {("20m", "ExporterResourceHigh"): [],
             ("35m", "ExporterResourceHigh"): [({"instance": INST_A}, None)]}
    sc.append(("S14 fd leak (150 open fds): ResourceHigh fires after its 30m "
               "`for`, not at 20m.", out,
               checks(["20m", "35m"], ["ExporterResourceHigh"], fired)))

    n = 90
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", f"1x{n}"),
             s("camera_motion_intervals_found", f"8x{n}"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds",
               "-7080+60x50 -4080x40"),
             s("frigate_response_seconds_count", "0+1x2 2x88", camera=None,
               kind="json"),
             s("frigate_response_seconds_count", "0+1x50 50x40", camera=None,
               kind="clip_ttfb"),
             s("process_start_time_seconds", "-43200x90", camera=None),
             s("process_open_fds", "12x90", camera=None),
             s("process_resident_memory_bytes", "96075776x90", camera=None)]
    fired = {("45m", "NoFrigateQueries"): [],
             ("45m", "ArchiveStalledWithMotion"): [],
             ("75m", "NoFrigateQueries"): [({}, None)]}
    sc.append(("S15 long catch-up: JSON frozen from 2m while clip_ttfb streams "
               "for 50 min (a 45-min silent-JSON window is NORMAL); "
               "NoFrigateQueries and Stalled stay silent. At 50m the stream "
               "dies too - both increases hit zero and the rule fires 20 min "
               "later.", lines,
               checks(["45m", "75m"], ["NoFrigateQueries",
                                       "ArchiveStalledWithMotion"], fired)))

    # S16: Frigate's clock runs 30 min ahead; every watermark it reports is
    # in the future, the archiver legitimately waits, and every time() - wm
    # rule computes a negative lag and goes silent. Only WatermarkInFuture
    # speaks up.
    n = 60
    lines = [s("up", f"1x{n}", camera=None),
             s("camera_task_running", f"1x{n}"),
             s("camera_motion_intervals_found", f"0x{n}"),
             s("camera_consecutive_failures", f"0x{n}"),
             s("camera_watermark_timestamp_seconds", f"1800+60x{n-1}"),
             s("frigate_response_seconds_count", f"0+1x{n}", camera=None,
               kind="json"),
             s("frigate_response_seconds_count", f"0+1x{n}", camera=None,
               kind="clip_ttfb"),
             s("process_start_time_seconds", f"-43200x{n}", camera=None),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    fired = {("5m", "WatermarkInFuture"): [],
             ("15m", "WatermarkInFuture"): [({}, None)],
             ("15m", "ArchiveLagHigh"): []}
    sc.append(("S16 Frigate clock 30 min ahead (NTP broken): watermark lives "
               "in the future, Lag computes negative and stays silent the "
               "whole time, WatermarkInFuture fires after its for.", lines,
               checks(["5m", "15m"], ["WatermarkInFuture", "ArchiveLagHigh"],
                      fired)))

    # S17: Frigate unreachable at startup. Metrics server binds before
    # discovery, so the pod IS scrapeable and up=1 during each ~65 s life;
    # discovery burns its 12x5s budget and sys.exit(1) fires; k8s restarts the
    # container. No camera task ever exists, so camera_* series are absent and
    # every camera-scoped rule is silent by construction; failed discovery
    # calls still observe kind="json" (frigate_get's finally), so
    # NoFrigateQueries is silent too. ProcessRestartLoop is the detector.
    n = 45
    lines = [s("up", f"1x{n}", camera=None),
             s("process_start_time_seconds",
               "-600x1 0x1 60x2 150x1 240x2 390x2 540x36", camera=None),
             s("frigate_response_seconds_count",
               "0+12x2 12+12x3 48+12x5 108+12x9 216+12x26", camera=None,
               kind="json"),
             s("process_open_fds", f"12x{n}", camera=None),
             s("process_resident_memory_bytes", f"96075776x{n}", camera=None)]
    fired = {("10m", "ProcessRestartLoop"): [({"instance": INST_A}, None)],
             ("10m", "FrigateMotionExporterDown"): [],
             ("10m", "FrigateMotionExporterUnscrapeable"): [],
             ("10m", "NoFrigateQueries"): [],
             ("10m", "ArchiveTaskWedgedOrDead"): []}
    sc.append(("S17 Frigate dead at startup: discovery exhausts 12x5s, exit(1)"
               ", k8s restarts. up stays 1 (server binds before discovery), so"
               " Down/Unscrapeable cannot see it; no camera series exist, so no"
               " camera rule can fire; failed calls still count JSON, so rule 7"
               " is silent. ProcessRestartLoop is what pages.", lines,
               checks(["10m"], ["ProcessRestartLoop",
                                "FrigateMotionExporterDown",
                                "FrigateMotionExporterUnscrapeable",
                                "NoFrigateQueries",
                                "ArchiveTaskWedgedOrDead"], fired)))

    # S18: steady-state crash-loop under kubelet backoff (post-cap regime).
    # Frigate stays down: each container lives ~62 s (metrics server binds,
    # discovery burns 12x5s, sys.exit(1)), kubelet waits its capped 300 s and
    # restarts; the 10m "successful run" that resets the backoff never
    # happens, so this 1-alive : 5-dead rhythm is permanent. Series during a
    # dead gap: up=0; process_start keeps its last value inside the 5m
    # staleness window, then the new container starts with a new value.
    up_vals, ps_vals, json_vals = [], [], []
    for m in range(40):
        alive = m % 6 == 0  # ~62 s alive + 300 s backoff ~ alive 1 min in 6
        up_vals.append("1" if alive else "0")
        # exporter metrics exist only during the alive minute: gaps otherwise
        ps_vals.append(str(3600 * (m // 6 * 6)) if alive else "_")
        json_vals.append(str(12 * (m // 6)) if alive else "_")
    lines = [s("up", " ".join(up_vals), camera=None),
             s("process_start_time_seconds", " ".join(ps_vals), camera=None),
             s("frigate_response_seconds_count", " ".join(json_vals),
               camera=None, kind="json")]
    fired = {("12m", "ProcessRestartLoop"): [],
             # threshold > 2 needs a third start inside the window: with one
             # start per ~6 minutes it fires from ~14m on; at 12m the window
             # holds exactly 2 changes.
             # 12m is an alive minute (up=1); Down fires only inside dead gaps
             # (4m, 10m, ...) and its for resets at every restart, so at 12m
             # and 20m it is pending or off.
             ("12m", "FrigateMotionExporterDown"): [],
             ("20m", "ProcessRestartLoop"): [({"instance": INST_A}, None)],
             ("20m", "FrigateMotionExporterDown"): [],
             ("20m", "FrigateMotionExporterUnscrapeable"): []}
    fired[("4m", "FrigateMotionExporterDown")] = [({"instance": INST_A}, None)]
    sc.append(("S18 post-cap crash-loop: 62 s alive + 300 s kubelet backoff, "
               "forever. RestartLoop fires (every ~6 min a new start, 5+ "
               "changes per 30m window - it never resolves while Frigate is "
               "down); Down joins at the gaps: each dead gap is 5 min > 3m "
               "for. Unscrapeable stays silent: the 5m gaps never starve the "
               "5m stale window long enough to keep absent() pending 3m.",
               lines,
               checks(["4m", "12m", "20m"], ["ProcessRestartLoop",
                                       "FrigateMotionExporterDown",
                                       "FrigateMotionExporterUnscrapeable"],
                      fired)))

    return sc


def main():
    sev, ann = load_annotations()
    sc = build()
    out = ["# promtool unit tests for alerts-k8s.yml.",
           "# Run: promtool test rules prometheus/alerts-k8s_test.yml",
           "#",
           "# Each scenario feeds only metric shapes run_camera/supervise_camera",
           "# can actually produce; the header states expected behavior and the",
           "# minutes where it is pinned down.",
           "rule_files:", "  - alerts-k8s.yml", "evaluation_interval: 1m",
           "# The evaluation clock starts at zero, so process_start_time_seconds",
           "# values are expressed relative to it (negative = started before the",
           "# first sample), which is what age guards compare against.",
           "tests:"]
    for title, lines, checks_ in sc:
        out.append(f"\n  # {title}")
        out.append("  - interval: 1m")
        if lines:
            out.append("    input_series:")
            out.extend(lines)
        else:
            out.append("    input_series: []")
        out.append("    alert_rule_test:")
        for t, a, fired in checks_:
            out.append(f"      - eval_time: {t}")
            out.append(f"        alertname: {a}")
            if not fired:
                out.append("        exp_alerts: []")
                continue
            out.append("        exp_alerts:")
            for labels, inst in fired:
                base = RULE_LABELS[a](CAM, inst or INST_A)
                lbls = {"severity": sev[a], **base}
                out.append("          - exp_labels:")
                for k in sorted(lbls):
                    out.append(f'              {k}: "{lbls[k]}"')
                s_text, d_text = ann[a]
                out.append("            exp_annotations:")
                out.append(f'              summary: "{render(s_text, lbls)}"')
                out.append(f'              description: "{render(d_text, lbls)}"')
    open(OUT, "w").write("\n".join(out) + "\n")
    print(f"wrote {OUT}: {len(sc)} scenarios")


if __name__ == "__main__":
    main()

