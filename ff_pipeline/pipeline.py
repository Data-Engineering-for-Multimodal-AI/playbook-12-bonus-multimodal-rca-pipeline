"""
The whole pipeline in one streaming pass:

  1. Ingest      the Future Factories analog data (bundled sample or the downloaded 29 hours)
  2. Stream      events through an in-process queue or Kafka
  3. Process     close each cycle into a summary (R03's grasps, sensor means)
  4. Edge        diagnose each cycle next to the line; forward summaries, not raw events
  5. Lake        bronze/silver/gold Parquet; score the diagnoses against the labels
  6. Observe     report.json and dashboard.txt

    python demo_pipeline.py [--data sample|full] [--transport memory|kafka] [--speed N]
"""

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Dict, List, Optional

from ff_pipeline import data, rca
from ff_pipeline.edge import EdgeGateway
from ff_pipeline.lake import Lake
from ff_pipeline.processor import CycleProcessor
from ff_pipeline.transport import make_transport

STEPS = ["Ingest", "Stream", "Process", "Edge", "Lake and analytics", "Observe"]


def run(source: str = "auto", transport: str = "memory", speed: float = 0.0, max_cycles: Optional[int] = None,
        output: Optional[str] = None, config_path: Optional[str] = None) -> Dict:
    config = data.load_config(config_path)
    path = data.resolve(source)
    frame = data.load(source, max_cycles)
    labels = data.labels_by_cycle(frame)
    output_dir = Path(output) if output else data.ROOT / "output" / ("full" if path == data.FULL else "sample")
    output_dir.mkdir(parents=True, exist_ok=True)

    template = rca.load_template(data.ROOT / config["template"]["path"])
    tolerance = config["template"]["match_tolerance_s"]
    generic = rca.GenericDetector.load(data.ROOT / config["baseline"]["path"], config["baseline"]["z_threshold"])
    commissioning = {ts for stamps in template["commissioned_from"].values() for ts in stamps}

    report: Dict = {"steps": {}}
    report["steps"]["Ingest"] = {"source": str(path.relative_to(data.ROOT)), **data.summary(frame)}

    channel = make_transport(transport, config)
    processor = CycleProcessor(config)
    edge = EdgeGateway()
    lake = Lake(output_dir / "lake")
    diagnoses: List[Dict] = []
    generic_scores: List[Dict] = []
    close_ms: List[float] = []
    incomplete = 0

    def handle(summary) -> None:
        nonlocal incomplete
        if summary is None:
            return
        if not summary.complete:
            incomplete += 1
            return
        started = time.perf_counter()
        diagnosis = rca.diagnose(summary, template, tolerance)
        close_ms.append(summary.processing_ms + (time.perf_counter() - started) * 1000)
        diagnoses.append(diagnosis)
        generic_scores.append({"start_ts": summary.start_ts, **generic.score(summary)})
        edge.forward(summary, diagnosis)

    started = time.perf_counter()
    for event in channel.stream(data.events(frame), speed):
        lake.add_event(event)
        edge.observe(event)
        handle(processor.process(event))
    handle(processor.flush())
    elapsed = time.perf_counter() - started

    report["steps"]["Stream"] = {"transport": channel.name, "events": processor.events,
                                 "seconds": round(elapsed, 1), "events_per_second": round(processor.events / elapsed),
                                 "replay_speed": speed or "as fast as possible"}
    counts: Dict[int, int] = {}
    for message in edge.forwarded:
        counts[len(message["grasps"])] = counts.get(len(message["grasps"]), 0) + 1
    report["steps"]["Process"] = {
        "cycles_closed": len(diagnoses) + incomplete, "complete_cycles": len(diagnoses),
        "incomplete_segments": incomplete,
        "r03_grasps_per_cycle": dict(sorted(counts.items(), reverse=True)),
        "close_and_diagnose_ms_p50": round(statistics.median(close_ms), 3) if close_ms else None,
        "close_and_diagnose_ms_max": round(max(close_ms), 3) if close_ms else None,
    }
    report["steps"]["Edge"] = {**edge.stats(),
                               "warnings": sum("warning" in d for d in diagnoses)}

    scored = [d for d in diagnoses if d["start_ts"] not in commissioning]
    mismatches = [{"cycle_count": d["cycle_count"], "start": _utc(d["start_ts"]),
                   "label": labels[d["start_ts"]] or "complete",
                   "diagnosis": ",".join(d["missing"]) or "complete",
                   "commissioning_cycle": d["start_ts"] in commissioning}
                  for d in diagnoses if sorted(d["missing"]) != rca.truth(labels[d["start_ts"]]) or d["unexpected_grasps"]]
    report["steps"]["Lake and analytics"] = {
        "lake": lake.finish(edge.forwarded),
        "process_aware_rca": rca.evaluate(diagnoses, labels),
        "process_aware_rca_without_commissioning_cycles": rca.evaluate(scored, labels) if scored else None,
        "generic_detector": rca.evaluate(generic_scores, labels),
        "generic_detector_top_sensors": _top([g["top_sensor"] for g in generic_scores if g["defect"]]),
        "rca_mismatches": mismatches,
    }
    report["examples"] = _examples(diagnoses, labels)
    report["output"] = str(output_dir.relative_to(data.ROOT)) if output_dir.is_relative_to(data.ROOT) else str(output_dir)
    with open(output_dir / "report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    dashboard = render(report)
    (output_dir / "dashboard.txt").write_text(dashboard)
    report["steps"]["Observe"] = {"report": f"{report['output']}/report.json", "dashboard": f"{report['output']}/dashboard.txt"}
    return report


def _utc(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts / 1000)) + " UTC"


def _top(values: List[str], n: int = 3) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1])[:n])


def _examples(diagnoses: List[Dict], labels: Dict[int, str]) -> List[Dict]:
    """The first cycle of each kind of diagnosis, for the dashboard."""
    seen, out = set(), []
    for d in diagnoses:
        kind = ",".join(d["missing"]) or ("warning" if "warning" in d else "complete")
        if kind not in seen:
            seen.add(kind)
            out.append({"cycle_count": d["cycle_count"], "start": _utc(d["start_ts"]),
                        "label": labels[d["start_ts"]] or "complete",
                        "finding": d.get("root_cause") or d.get("warning") or "complete rocket"})
    return out


def render(report: Dict) -> str:
    s = report["steps"]
    rca_all = s["Lake and analytics"]["process_aware_rca"]
    rca_scored = s["Lake and analytics"]["process_aware_rca_without_commissioning_cycles"] or rca_all
    gen = s["Lake and analytics"]["generic_detector"]
    lines = ["=" * 72, "FUTURE FACTORIES STREAMING RCA: DASHBOARD", "=" * 72,
             f"Data:      {s['Ingest']['source']}: {s['Ingest']['rows']:,} events, {s['Ingest']['hours']} h, "
             f"{s['Process']['complete_cycles']} complete cycles",
             f"Stream:    {s['Stream']['transport']}, {s['Stream']['events_per_second']:,} events/s "
             f"({s['Stream']['seconds']} s)",
             f"Edge:      {s['Edge']['raw_mb']} MB of raw events -> {s['Edge']['forwarded_mb']} MB forwarded "
             f"({s['Edge']['reduction_percent']}% less); {s['Edge']['alerts']} defect alerts, "
             f"{s['Edge']['warnings']} timing warnings",
             "", "Root cause analysis against the labels (cycles scored, excluding the commissioning cycles):",
             f"  Process-aware (R03 grasps):  precision {rca_scored['precision']}, recall {rca_scored['recall']}, "
             f"missing pieces exactly right {rca_scored.get('missing_pieces_exact')} ({rca_scored['cycles']} cycles)",
             f"  Generic (sensor z-scores):   precision {gen['precision']}, recall {gen['recall']} "
             f"({gen['false_positives']} complete rockets flagged; top sensor "
             f"{next(iter(s['Lake and analytics']['generic_detector_top_sensors']), '-')})",
             "", "Label -> diagnosis (all cycles):"]
    for label, row in rca_all.get("confusion", {}).items():
        lines.append(f"  {label:<24} " + ", ".join(f"{k}: {v}" for k, v in row.items()))
    if s["Lake and analytics"]["rca_mismatches"]:
        lines += ["", "Cycles where the diagnosis differs from the label:"]
        for m in s["Lake and analytics"]["rca_mismatches"]:
            lines.append(f"  cycle {m['cycle_count']} at {m['start']}: label {m['label']}, diagnosis {m['diagnosis']}"
                         + (" (commissioning cycle)" if m["commissioning_cycle"] else ""))
    lines += ["", "Examples:"]
    for e in report["examples"]:
        lines.append(f"  cycle {e['cycle_count']} ({e['label']}): {e['finding']}")
    lines.append("=" * 72)
    return "\n".join(lines)


def print_step(report: Dict, step: int) -> None:
    name = STEPS[step - 1]
    print(f"\nSTEP {step}: {name.upper()}")
    print("-" * 72)
    if name == "Observe":
        print(render(report))
        print(f"\nWritten: {report['steps']['Observe']['report']}, {report['steps']['Observe']['dashboard']}")
        return
    print(json.dumps(report["steps"][name], indent=2, default=str))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", default="auto", help="sample, full, or auto (full if downloaded; default)")
    parser.add_argument("--transport", default="memory", choices=["memory", "kafka"])
    parser.add_argument("--speed", type=float, default=0.0, help="replay at N times real time (default: as fast as possible)")
    parser.add_argument("--max-cycles", type=int, help="stop after this many cycles")
    parser.add_argument("--output", help="output folder (default: output/sample or output/full)")
    parser.add_argument("--step", type=int, choices=range(1, 7), help="show only this step's output")
    args = parser.parse_args(argv)
    report = run(args.data, args.transport, args.speed, args.max_cycles, args.output)
    for step in ([args.step] if args.step else range(1, 7)):
        print_step(report, step)
    return 0
