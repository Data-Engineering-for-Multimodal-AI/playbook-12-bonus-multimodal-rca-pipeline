#!/usr/bin/env python3
"""
Look at one assembly cycle: R03's gripper load over the cycle, the grasps found, and which template
slot (piece) each one fills.

    python scripts/cycle.py 14                 # cycle 14 of the bundled sample
    python scripts/cycle.py 14 --data full     # of the downloaded 29 hours
    python scripts/cycle.py                    # list the cycles
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ff_pipeline import data, rca  # noqa: E402
from ff_pipeline.processor import CycleProcessor  # noqa: E402

BARS = " .:-=+*#%@"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("cycle", nargs="?", type=int, help="the cell's cycle counter (Q_Cell_CycleCount)")
    parser.add_argument("--data", default="sample", help="sample (default), full, or a .parquet path")
    args = parser.parse_args(argv)
    config = data.load_config()
    frame = data.load(args.data)
    ids = data.cycle_ids(frame)
    counts = frame.groupby(ids)["Q_Cell_CycleCount"].first()
    labels = frame.groupby(ids)[data.LABEL].first().replace("", "complete")
    if args.cycle is None or args.cycle not in set(counts):
        if args.cycle is not None:
            print(f"No cycle {args.cycle} in {args.data}.")
        print("Cycles: " + ", ".join(f"{c} ({labels[i]})" for i, c in counts.items()))
        return 0 if args.cycle is None else 1
    part = frame[ids == counts[counts == args.cycle].index[0]].reset_index(drop=True)
    processor = CycleProcessor(config)
    for event in data.events(part):
        processor.process(event)
    summary = processor.flush()
    template = rca.load_template(ROOT / config["template"]["path"])
    tolerance = config["template"]["match_tolerance_s"]
    m = rca.match(summary, template, tolerance)
    diagnosis = rca.diagnose(summary, template, tolerance)

    load = part["I_R03_Gripper_Load"].to_numpy()
    seconds = (part["ts_ms"] - part["ts_ms"].iloc[0]).to_numpy() / 1000
    print(f"Cycle {args.cycle}, label {labels[counts[counts == args.cycle].index[0]]}, "
          f"{summary.duration_s:.0f} s, {summary.samples:,} samples")
    print(f"\nR03 gripper load, one character per 2 s from 50 s to 130 s ('@' = {load.max():,.0f}):")
    line = ""
    for start in range(50, 130, 2):
        window = load[(seconds >= start) & (seconds < start + 2)]
        level = 0 if len(window) == 0 else int((window.max() - load.min()) / (load.max() - load.min() + 1e-9) * 9)
        line += BARS[level]
    print(f"  50 s |{line}| 130 s")
    print(f"\nTemplate slot  piece   expected  found     (cycle shifted {m['shift_s']:+.1f} s)")
    for slot, (piece, expected) in enumerate(zip(template["pieces"], template["start_s"])):
        grasp = m["slots"].get(slot)
        found = f"{summary.grasps[grasp].start_s:6.1f} s" if grasp is not None else "nothing"
        print(f"  {slot}            {piece:<6}  {expected + m['shift_s']:6.1f} s  {found}")
    print(f"\nDiagnosis: {diagnosis.get('root_cause') or diagnosis.get('warning') or 'complete rocket'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
