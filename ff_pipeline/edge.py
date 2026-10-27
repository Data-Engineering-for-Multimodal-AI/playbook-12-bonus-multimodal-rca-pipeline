"""
Stage 4: Edge. The stream processor and the RCA run next to the line; only cycle summaries and their
diagnoses go upstream, and the raw 10 Hz events stay in the local bronze layer. This counts the bytes
both ways: what the raw events would take as JSON, and what is actually forwarded.
"""

import json
from dataclasses import asdict
from typing import Dict, List

from ff_pipeline.processor import CycleSummary


class EdgeGateway:
    def __init__(self):
        self.raw_bytes = 0
        self.raw_events = 0
        self.forwarded: List[Dict] = []
        self.forwarded_bytes = 0
        self.alerts = 0

    def observe(self, event: Dict) -> None:
        self.raw_events += 1
        self.raw_bytes += len(json.dumps(event))

    def forward(self, summary: CycleSummary, diagnosis: Dict) -> Dict:
        message = {
            "cycle_count": summary.cycle_count,
            "start_ts": summary.start_ts,
            "end_ts": summary.end_ts,
            "samples": summary.samples,
            "grasps": [asdict(g) for g in summary.grasps],
            "sensor_means": {k: round(v, 4) for k, v in summary.sensor_means.items()},
            "diagnosis": diagnosis,
        }
        self.forwarded.append(message)
        self.forwarded_bytes += len(json.dumps(message))
        self.alerts += bool(diagnosis.get("defect"))
        return message

    def stats(self) -> Dict:
        return {
            "raw_events": self.raw_events,
            "raw_mb": round(self.raw_bytes / 1e6, 1),
            "forwarded_messages": len(self.forwarded),
            "forwarded_mb": round(self.forwarded_bytes / 1e6, 3),
            "reduction_percent": round(100 * (1 - self.forwarded_bytes / self.raw_bytes), 2) if self.raw_bytes else None,
            "alerts": self.alerts,
        }
