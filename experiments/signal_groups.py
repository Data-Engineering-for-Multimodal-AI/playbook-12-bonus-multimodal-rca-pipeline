"""Try this 3: which signals show a missing piece? (make try-signal-groups)

The generic detector (ff_pipeline/rca.py) flags a cycle when any of its 45 sensor signals averages
far from the first 10 cycles of the recording (z-score above 4). This runs the same detector on one
group of signals at a time, the conveyor, the cell's safety signals and each robot, over the bundled
12 cycles, and scores it against the labels next to the process-aware diagnosis. No Docker needed.

Change Z_THRESHOLD or GROUPS below and run it again.
"""

import sys

from common import CONFIG, ROOT, TEMPLATE, TOLERANCE, data, fmt, rca, summaries

Z_THRESHOLD = CONFIG["baseline"]["z_threshold"]
GROUPS = {
    "all 45 signals": lambda k: True,
    "conveyor": lambda k: "VFD" in k or "Stopper" in k,
    "cell safety and tray": lambda k: any(x in k for x in ("Safety", "EStop", "Tray")),
    "R01 (carries the tray)": lambda k: "_R01_" in k,
    "R02 (body pieces)": lambda k: "_R02_" in k,
    "R03 (builds the rocket)": lambda k: "_R03_" in k,
    "R04": lambda k: "_R04_" in k,
}


def main() -> int:
    frame = data.load(sys.argv[1] if len(sys.argv) > 1 else "sample")
    labels = data.labels_by_cycle(frame)
    cycles = summaries(frame)
    baseline = rca.GenericDetector.load(ROOT / CONFIG["baseline"]["path"], Z_THRESHOLD)
    defects = sum(bool(labels[s.start_ts]) for s in cycles)
    print(f"{len(cycles)} cycles: {defects} with pieces missing, {len(cycles) - defects} complete rockets\n")
    print(f"{'signals':<26}  {'count':>5}  {'flagged':>7}  {'false alarms':>12}  {'precision':>9}  {'recall':>6}  top signal")
    for name, keep in GROUPS.items():
        idx = [i for i, k in enumerate(baseline.keys) if keep(k)]
        detector = rca.GenericDetector([baseline.keys[i] for i in idx], baseline.mean[idx], baseline.std[idx],
                                       Z_THRESHOLD)
        scores = [{"start_ts": s.start_ts, **detector.score(s)} for s in cycles]
        result = rca.evaluate(scores, labels)
        flagged = [s for s in scores if s["defect"]]
        top = max({s["top_sensor"] for s in flagged}, key=[s["top_sensor"] for s in flagged].count) if flagged else "-"
        print(f"{name:<26}  {len(idx):>5}  {len(flagged):>7}  {result['false_positives']:>12}  "
              f"{fmt(result['precision']):>9}  {fmt(result['recall']):>6}  {top}")
    rca_result = rca.evaluate([rca.diagnose(s, TEMPLATE, TOLERANCE) for s in cycles], labels)
    print(f"{'process-aware (R03 grasps)':<26}  {1:>5}  {rca_result['true_positives'] + rca_result['false_positives']:>7}  "
          f"{rca_result['false_positives']:>12}  {fmt(rca_result['precision']):>9}  {fmt(rca_result['recall']):>6}  "
          "I_R03_Gripper_Load")
    return 0


if __name__ == "__main__":
    sys.exit(main())
