"""Try this 1: how often do you need to sample the line? (make try-sample-rate)

The recording is 10 Hz. This keeps every 2nd, 5th, 10th ... sample of the bundled 12 cycles, as if the
line were recorded more slowly, and runs the cycle processor and the process-aware diagnosis on each
version. It reports how many of R03's grasps survive, whether the missing pieces are still named
exactly, and how much raw data the line would send. No Docker needed.

The grasp detector needs `min_samples` (config/pipeline.json, 3) samples above the load threshold
in a row. Change MIN_SAMPLES below and run it again.
"""

import json
import sys

from common import data, config_with, fmt, score, summaries

EVERY = [1, 2, 5, 10, 20, 30, 50]
MIN_SAMPLES = None   # None: the value in config/pipeline.json


def main() -> int:
    frame = data.load(sys.argv[1] if len(sys.argv) > 1 else "sample")
    labels = data.labels_by_cycle(frame)
    config = config_with(min_samples=MIN_SAMPLES) if MIN_SAMPLES else None
    print(f"{len(frame):,} samples at 10 Hz, {len(labels)} cycles\n")
    print(f"{'rate':>7}  {'samples':>7}  {'raw MB':>6}  {'R03 grasps':>10}  {'precision':>9}  {'recall':>6}  "
          f"{'pieces exactly right':>20}")
    for n in EVERY:
        part = frame.iloc[::n].reset_index(drop=True)
        raw_mb = sum(len(json.dumps(e)) for e in data.events(part)) / 1e6
        result = score(summaries(part, config), data.labels_by_cycle(part))
        print(f"{f"{10 / n:.2g} Hz":>7}  {len(part):>7,}  {raw_mb:>6.1f}  {result['grasps']:>10}  "
              f"{fmt(result.get('precision')):>9}  {fmt(result.get('recall')):>6}  "
              f"{fmt(result.get('missing_pieces_exact')):>20}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
