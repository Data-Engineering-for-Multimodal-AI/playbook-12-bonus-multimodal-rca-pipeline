"""
Stage 4 (at the edge): root cause analysis per cycle.

Process-aware: a complete rocket means R03 grips four times, at about the same moments in every
cycle. The template records when (learned from the first cycles, without labels). Commissioning then
names each grasp after the piece whose absence makes it disappear, from one known-faulty cycle of each
kind, as an engineer would by removing each piece once. A cycle's grasps are matched to the template
after shifting the cycle in time to fit best (some cycles run a few seconds late); a grasp that isn't
there means R03 reached for that piece and found nothing: the piece was missing from the tray.

Generic, for comparison: z-scores of every sensor's per-cycle mean against the calibration cycles,
the kind of detector that knows nothing about the process.

    python -m ff_pipeline.rca --learn-template   # learn and commission the template; calibrate the generic detector
"""

import argparse
import json
from itertools import combinations
from statistics import median
from typing import Dict, List, Optional

import numpy as np

from ff_pipeline.data import ROOT
from ff_pipeline.processor import CycleSummary

LABEL_PIECES = {"NoNose": "nose", "NoBody1": "body1", "NoBody2": "body2"}


def learn_template(summaries: List[CycleSummary]) -> Dict:
    """Grasp times of a normal cycle: the most common number of grasps, and their median start times."""
    complete = [s for s in summaries if s.complete]
    counts = [len(s.grasps) for s in complete]
    usual = max(set(counts), key=counts.count)
    normal = [s for s in complete if len(s.grasps) == usual]
    return {
        "start_s": [round(median(s.grasps[i].start_s for s in normal), 1) for i in range(usual)],
        "duration_s": [round(median(s.grasps[i].duration_s for s in normal), 1) for i in range(usual)],
        "calibration_cycles": len(complete),
        "cycles_used": len(normal),
    }


def match(summary: CycleSummary, template: Dict, tolerance_s: float) -> Dict:
    """Which template slots the cycle's grasps fill. Grasps come in the template's order, and a missing
    piece leaves a gap, so try every in-order choice of slots for the grasps seen and keep the one whose
    spacing fits best (after shifting the whole cycle, since some cycles run late)."""
    starts = [g.start_s for g in summary.grasps]
    slots = template["start_s"]
    if not starts or len(starts) > len(slots):
        return {"shift_s": 0.0, "slots": {}, "missing_slots": list(range(len(slots))) if not starts else [],
                "extra": starts[len(slots):] if starts else [], "late": []}
    best = None
    for chosen in combinations(range(len(slots)), len(starts)):
        residuals = [t - slots[i] for t, i in zip(starts, chosen)]
        shift = median(residuals)
        cost = sum(abs(r - shift) for r in residuals)
        if best is None or (cost, abs(shift)) < (best[0], abs(best[1])):
            best = (cost, shift, chosen, residuals)
    _, shift, chosen, residuals = best
    late = [round(r - shift, 1) for r in residuals]
    return {"shift_s": round(shift, 1), "slots": dict(zip(chosen, range(len(starts)))),
            "missing_slots": [i for i in range(len(slots)) if i not in chosen], "extra": [],
            # grasps more than the tolerance away from where the template puts them
            "late": [(template.get("pieces", [str(i) for i in range(len(slots))])[i], d)
                     for i, d in zip(chosen, late) if abs(d) > tolerance_s]}


def commission(template: Dict, faulty: Dict[str, List[CycleSummary]], tolerance_s: float, pieces: List[str]) -> Dict:
    """Name the slots: each known fault removes some pieces; the slots that disappear are those pieces.
    Each fault is judged by the majority of its example cycles (a label can be off at a block's edge).
    The slot that never disappears is the piece that was never removed."""
    names: Dict[int, str] = {}
    for label, examples in sorted(faulty.items(), key=lambda kv: len(kv[0].split(","))):
        votes = [tuple(match(s, template, tolerance_s)["missing_slots"]) for s in examples]
        majority = max(set(votes), key=votes.count)
        gone = [i for i in majority if i not in names]
        new = [p for p in truth(label) if p not in names.values()]
        if len(gone) != 1 or len(new) != 1:
            raise ValueError(f"Can't commission from {label}: slots {gone} disappeared for pieces {new}")
        names[gone[0]] = new[0]
    remaining = [p for p in pieces if p not in names.values()]
    for i in range(len(template["start_s"])):
        if i not in names:
            names[i] = remaining.pop(0)
    template = dict(template)
    template["pieces"] = [names[i] for i in range(len(template["start_s"]))]
    template["commissioned_from"] = {label: [s.start_ts for s in examples] for label, examples in faulty.items()}
    return template


def diagnose(summary: CycleSummary, template: Dict, tolerance_s: float) -> Dict:
    m = match(summary, template, tolerance_s)
    missing = [template["pieces"][i] for i in m["missing_slots"]]
    result = {
        "cycle_count": summary.cycle_count,
        "start_ts": summary.start_ts,
        "defect": bool(missing or m["extra"]),
        "missing": missing,
        "unexpected_grasps": m["extra"],
        "shift_s": m["shift_s"],
        "late_grasps": m["late"],
    }
    if missing:
        first = min(m["missing_slots"], key=lambda i: template["start_s"][i])
        expected = template["start_s"][first] + m["shift_s"]
        result["root_cause"] = (f"{', '.join(missing)} missing from the tray: R03 found nothing to grip "
                                f"at about {expected:.0f} s into the cycle")
    elif m["extra"]:
        result["root_cause"] = f"R03 gripped {len(m['extra'])} more time(s) than a complete rocket needs"
    elif m["late"]:
        result["warning"] = "complete, but " + ", ".join(f"the {p} grasp came {d:+.1f} s off schedule" for p, d in m["late"])
    return result


class GenericDetector:
    """Flags a cycle when any sensor's cycle mean is far from the calibration cycles' mean."""

    def __init__(self, keys: List[str], mean: List[float], std: List[float], z_threshold: float):
        self.keys = keys
        self.mean = np.array(mean)
        self.std = np.array(std)
        self.z_threshold = z_threshold

    @classmethod
    def calibrate(cls, calibration: List[CycleSummary], z_threshold: float) -> "GenericDetector":
        keys = sorted(set().union(*(s.sensor_means for s in calibration)))
        values = np.array([[s.sensor_means.get(k, np.nan) for k in keys] for s in calibration])
        mean = np.nanmean(values, axis=0)
        # Signals that didn't move during calibration still count when they move later
        std = np.maximum(np.nanstd(values, axis=0), 1e-6 * np.maximum(np.abs(mean), 1.0))
        return cls(keys, mean.tolist(), std.tolist(), z_threshold)

    @classmethod
    def load(cls, path, z_threshold: float) -> "GenericDetector":
        with open(path) as f:
            saved = json.load(f)
        return cls(saved["sensors"], saved["mean"], saved["std"], z_threshold)

    def save(self, path, source: str) -> None:
        with open(path, "w") as f:
            json.dump({"source": source, "sensors": self.keys, "mean": [round(float(v), 6) for v in self.mean],
                       "std": [float(f"{v:.6g}") for v in self.std]}, f, indent=1)

    def score(self, summary: CycleSummary) -> Dict:
        x = np.array([summary.sensor_means.get(k, np.nan) for k in self.keys])
        z = np.abs(x - self.mean) / self.std
        worst = int(np.nanargmax(z))
        return {"defect": bool(np.nanmax(z) > self.z_threshold), "max_z": round(float(np.nanmax(z)), 1),
                "top_sensor": self.keys[worst]}


def truth(label: str) -> List[str]:
    return sorted(LABEL_PIECES[p] for p in label.split(",") if p in LABEL_PIECES)


def evaluate(results: List[Dict], labels: Dict[int, str], key: str = "defect") -> Dict:
    """Detection precision and recall, and for the RCA, the share of cycles whose missing pieces are exactly right."""
    tp = fp = fn = tn = 0
    exact = 0
    confusion: Dict[str, Dict[str, int]] = {}
    for r in results:
        label = labels[r["start_ts"]]
        actual = bool(label)
        predicted = r[key]
        tp += actual and predicted
        fp += predicted and not actual
        fn += actual and not predicted
        tn += not actual and not predicted
        if "missing" in r:
            exact += sorted(r["missing"]) == truth(label)
            row = confusion.setdefault(label or "complete", {})
            found = ",".join(r["missing"]) or ("unexpected grasp" if r["unexpected_grasps"] else "complete")
            row[found] = row.get(found, 0) + 1
    out = {"cycles": len(results), "true_positives": tp, "false_positives": fp, "false_negatives": fn,
           "true_negatives": tn,
           "precision": round(tp / (tp + fp), 3) if tp + fp else None,
           "recall": round(tp / (tp + fn), 3) if tp + fn else None}
    if confusion:
        out["missing_pieces_exact"] = round(exact / len(results), 3)
        out["confusion"] = confusion
    return out


def load_template(path: Optional[str] = None) -> Dict:
    with open(path or ROOT / "config" / "r03_template.json") as f:
        return json.load(f)


def main() -> None:
    from ff_pipeline import data
    from ff_pipeline.processor import CycleProcessor

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--learn-template", action="store_true",
                        help="record config/r03_template.json and config/generic_baseline.json")
    parser.add_argument("--cycles", type=int, default=10, help="calibration cycles from the start of the run")
    args = parser.parse_args()
    if not args.learn_template:
        parser.print_help()
        return
    config = data.load_config()
    frame = data.load("full")
    labels = data.labels_by_cycle(frame)
    processor = CycleProcessor(config)
    calibration: List[CycleSummary] = []
    faulty: Dict[str, List[CycleSummary]] = {}
    for event in data.events(frame):
        closed = processor.process(event)
        if not (closed and closed.complete):
            continue
        if len(calibration) < args.cycles:
            calibration.append(closed)
        label = labels[closed.start_ts]
        if label and len(faulty.setdefault(label, [])) < 3:
            faulty[label].append(closed)    # the first 3 cycles of each kind of fault
    tolerance = config["template"]["match_tolerance_s"]
    template = learn_template(calibration)
    template = commission(template, faulty, tolerance, config["template"]["pieces"])
    template["source"] = (f"grasp times: first {args.cycles} complete cycles of the Future Factories analog dataset "
                          "(no labels); piece names: the first 3 cycles of each fault (commissioned_from)")
    with open(ROOT / config["template"]["path"], "w") as f:
        json.dump(template, f, indent=2)
    print(json.dumps(template, indent=2))
    generic = GenericDetector.calibrate(calibration, config["baseline"]["z_threshold"])
    generic.save(ROOT / config["baseline"]["path"],
                 f"per-cycle sensor means of the first {args.cycles} complete cycles (no labels)")
    print(f"Generic detector baseline: {len(generic.keys)} sensors -> {config['baseline']['path']}")


if __name__ == "__main__":
    main()
