"""
Stage 5: Data lake. Three layers of Parquet files:

  bronze/  the raw events as received, partitioned by date and hour
  silver/  one row per cycle: summary and diagnosis
  gold/    per hour: cycles, defects, defect rate, which pieces were missing
"""

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


class Lake:
    def __init__(self, root: Path, batch: int = 50_000):
        self.root = Path(root)
        self.batch = batch
        self.buffer: List[Dict] = []
        self.files = 0

    def add_event(self, event: Dict) -> None:
        self.buffer.append(event)
        if len(self.buffer) >= self.batch:
            self._write_bronze()

    def _write_bronze(self) -> None:
        if not self.buffer:
            return
        frame = pd.DataFrame(self.buffer)
        stamp = pd.to_datetime(frame["ts_ms"], unit="ms", utc=True)
        frame["date"], frame["hour"] = stamp.dt.strftime("%Y-%m-%d"), stamp.dt.hour
        for (date, hour), part in frame.groupby(["date", "hour"]):
            folder = self.root / "bronze" / f"date={date}" / f"hour={hour:02d}"
            folder.mkdir(parents=True, exist_ok=True)
            part.drop(columns=["date", "hour"]).to_parquet(folder / f"part-{self.files:05d}.parquet", index=False)
            self.files += 1
        self.buffer = []

    def finish(self, cycles: List[Dict]) -> Dict:
        self._write_bronze()
        silver = pd.DataFrame([{
            "cycle_count": c["cycle_count"],
            "start": pd.to_datetime(c["start_ts"], unit="ms", utc=True),
            "duration_s": (c["end_ts"] - c["start_ts"]) / 1000,
            "samples": c["samples"],
            "grasps": len(c["grasps"]),
            "grasp_starts_s": json.dumps([g["start_s"] for g in c["grasps"]]),
            "defect": c["diagnosis"]["defect"],
            "missing": ",".join(c["diagnosis"]["missing"]),
            "root_cause": c["diagnosis"].get("root_cause", ""),
            "warning": c["diagnosis"].get("warning", ""),
        } for c in cycles])
        (self.root / "silver").mkdir(parents=True, exist_ok=True)
        silver.to_parquet(self.root / "silver" / "cycles.parquet", index=False)

        hourly = silver.assign(hour=silver["start"].dt.floor("h"))
        gold = hourly.groupby("hour").agg(cycles=("defect", "size"), defects=("defect", "sum"))
        gold["defect_rate"] = (gold["defects"] / gold["cycles"]).round(3)
        for piece in ("body1", "body2", "nose"):
            gold[f"missing_{piece}"] = hourly.groupby("hour")["missing"].apply(lambda s, p=piece: s.str.contains(p).sum())
        (self.root / "gold").mkdir(parents=True, exist_ok=True)
        gold.reset_index().to_parquet(self.root / "gold" / "hourly_quality.parquet", index=False)
        gold.reset_index().to_csv(self.root / "gold" / "hourly_quality.csv", index=False)
        return {"bronze_files": self.files, "silver_cycles": len(silver), "gold_hours": len(gold),
                "size_mb": round(sum(f.stat().st_size for f in self.root.rglob("*") if f.is_file()) / 1e6, 1)}
