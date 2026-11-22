"""Try this 2: where do you draw the line between "holding a piece" and "empty"? (make try-load-threshold)

R03's gripper load cell reads about 1,370 when empty and 4,000 to 10,000 while it holds a piece. The
cycle processor counts a grasp when the load stays above `load_threshold` (config/pipeline.json, 3000)
for `min_samples` samples in a row. This runs the bundled 12 cycles with other thresholds and reports
the grasps found and the diagnosis scores. No Docker needed.

Change THRESHOLDS below, or the cycles with a data path: python experiments/load_threshold.py full
"""

import sys

from common import data, config_with, fmt, score, summaries

THRESHOLDS = [1380, 1400, 1500, 3000, 5000, 7000, 9000]


def main() -> int:
    frame = data.load(sys.argv[1] if len(sys.argv) > 1 else "sample")
    labels = data.labels_by_cycle(frame)
    load = frame["I_R03_Gripper_Load"]
    print(f"R03 gripper load over {len(frame):,} samples: min {load.min():,}, median {load.median():,.0f}, "
          f"max {load.max():,}\n")
    print(f"{'threshold':>9}  {'R03 grasps':>10}  {'precision':>9}  {'recall':>6}  {'pieces exactly right':>20}")
    for threshold in THRESHOLDS:
        result = score(summaries(frame, config_with(load_threshold=threshold)), labels)
        mark = "  (config/pipeline.json)" if threshold == data.load_config()["grasp"]["load_threshold"] else ""
        print(f"{threshold:>9,}  {result['grasps']:>10}  {fmt(result.get('precision')):>9}  "
              f"{fmt(result.get('recall')):>6}  {fmt(result.get('missing_pieces_exact')):>20}{mark}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
