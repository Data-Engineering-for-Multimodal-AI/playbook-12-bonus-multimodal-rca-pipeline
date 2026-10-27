"""
Stage 3: Stream processing. Consumes sensor events one at a time and, at the end of every cycle,
emits a cycle summary: when R03 held a piece (its grasps), and every sensor's mean over the cycle.

A cycle ends when the cell's cycle counter changes or the stream pauses (data.GAP_MS), so a summary is
emitted on the first event of the next cycle, as a streaming job would.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from ff_pipeline.data import GAP_MS


@dataclass
class Grasp:
    start_s: float      # seconds after the cycle started
    duration_s: float
    peak_load: float


@dataclass
class CycleSummary:
    cycle_count: int
    start_ts: int
    end_ts: int
    samples: int
    grasps: List[Grasp]
    sensor_means: Dict[str, float]
    complete: bool
    processing_ms: float = 0.0   # time to close the cycle and build this summary

    @property
    def duration_s(self) -> float:
        return (self.end_ts - self.start_ts) / 1000


@dataclass
class _Open:
    cycle_count: int
    start_ts: int
    last_ts: int
    samples: int = 0
    sums: Dict[str, float] = field(default_factory=dict)
    counts: Dict[str, int] = field(default_factory=dict)
    grasps: List[Grasp] = field(default_factory=list)
    above: int = 0          # consecutive samples above the load threshold
    grasp_start: int = 0
    grasp_peak: float = 0.0


class CycleProcessor:
    def __init__(self, config: Dict):
        grasp = config["grasp"]
        self.load_column = f"I_{grasp['robot']}_Gripper_Load"
        self.threshold = grasp["load_threshold"]
        self.min_samples = grasp["min_samples"]
        self.min_seconds = config["cycle"]["min_seconds"]
        self.current: Optional[_Open] = None
        self.events = 0

    def process(self, event: Dict) -> Optional[CycleSummary]:
        """Add one event; returns the summary of the cycle it closed, if any."""
        self.events += 1
        closed = None
        cur = self.current
        if cur is not None and (event["Q_Cell_CycleCount"] != cur.cycle_count or event["ts_ms"] - cur.last_ts > GAP_MS):
            closed = self._close()
        if self.current is None:
            self.current = _Open(event["Q_Cell_CycleCount"], event["ts_ms"], event["ts_ms"])
        self._add(self.current, event)
        return closed

    def flush(self) -> Optional[CycleSummary]:
        return self._close() if self.current is not None else None

    def _add(self, cur: _Open, event: Dict) -> None:
        cur.samples += 1
        cur.last_ts = event["ts_ms"]
        for key, value in event.items():
            if key in ("ts_ms", "Q_Cell_CycleCount") or value is None:
                continue
            cur.sums[key] = cur.sums.get(key, 0.0) + float(value)
            cur.counts[key] = cur.counts.get(key, 0) + 1
        load = event.get(self.load_column)
        if load is not None and load > self.threshold:
            if cur.above == 0:
                cur.grasp_start, cur.grasp_peak = event["ts_ms"], load
            cur.above += 1
            cur.grasp_peak = max(cur.grasp_peak, load)
        else:
            self._end_grasp(cur, event["ts_ms"])

    def _end_grasp(self, cur: _Open, ts: int) -> None:
        if cur.above >= self.min_samples:
            cur.grasps.append(Grasp(round((cur.grasp_start - cur.start_ts) / 1000, 1),
                                    round((ts - cur.grasp_start) / 1000, 1), float(cur.grasp_peak)))
        cur.above = 0

    def _close(self) -> CycleSummary:
        started = time.perf_counter()
        cur = self.current
        self._end_grasp(cur, cur.last_ts)
        means = {k: cur.sums[k] / cur.counts[k] for k in cur.sums}
        summary = CycleSummary(cur.cycle_count, cur.start_ts, cur.last_ts, cur.samples, cur.grasps, means,
                               complete=(cur.last_ts - cur.start_ts) / 1000 >= self.min_seconds)
        summary.processing_ms = (time.perf_counter() - started) * 1000
        self.current = None
        return summary
