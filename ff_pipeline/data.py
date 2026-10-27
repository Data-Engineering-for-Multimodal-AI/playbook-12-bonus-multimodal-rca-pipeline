"""
Stage 1: Ingest. The Future Factories analog data as one table: 10 Hz samples from four robots, the
conveyor, the cell's cycle counter and safety signals. `Description` is the label (pieces removed from
the tray in that cycle); it is kept out of the event stream and used only to score the results.
"""

import json
from pathlib import Path
from typing import Dict, Iterator, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
FULL = ROOT / "data" / "ff-analog" / "ff_analog.parquet"
SAMPLE = ROOT / "data" / "sample" / "ff_sample.parquet"
LABEL = "Description"
GAP_MS = 10_000   # a pause this long ends a cycle (the recording has gaps where the line was stopped)


def cycle_ids(frame: pd.DataFrame) -> pd.Series:
    """Number the cycles: a new one starts when the cycle counter changes or after a gap."""
    new = (frame["Q_Cell_CycleCount"] != frame["Q_Cell_CycleCount"].shift()) | (frame["ts_ms"].diff() > GAP_MS)
    return new.cumsum()


def load_config(path: Optional[str] = None) -> Dict:
    with open(path or ROOT / "config" / "pipeline.json") as f:
        return json.load(f)


def resolve(data: str = "auto") -> Path:
    """'full' (downloaded), 'sample' (bundled) or 'auto' (full if downloaded)."""
    if data == "sample" or (data == "auto" and not FULL.exists()):
        return SAMPLE
    if data in ("full", "auto"):
        if not FULL.exists():
            raise FileNotFoundError("The full dataset isn't downloaded: python download_data.py")
        return FULL
    return Path(data)


def load(data: str = "auto", max_cycles: Optional[int] = None) -> pd.DataFrame:
    frame = pd.read_parquet(resolve(data))
    if max_cycles:
        frame = frame[cycle_ids(frame) <= max_cycles].reset_index(drop=True)
    return frame


def labels_by_cycle(frame: pd.DataFrame) -> Dict[int, str]:
    """Label of each cycle, keyed by the timestamp of its first sample."""
    first = frame.groupby(cycle_ids(frame)).agg(start=("ts_ms", "first"), label=(LABEL, "first"))
    return dict(zip(first["start"].astype(int), first["label"]))


def events(frame: pd.DataFrame) -> Iterator[Dict]:
    """Rows as sensor events, in time order, without the label."""
    columns = [c for c in frame.columns if c != LABEL]
    for row in frame[columns].itertuples(index=False, name=None):
        yield {c: (None if v != v else v.item() if hasattr(v, "item") else v) for c, v in zip(columns, row)}


def summary(frame: pd.DataFrame) -> Dict:
    cycle_id = cycle_ids(frame)
    labels = frame.groupby(cycle_id)[LABEL].first().replace("", "complete")
    return {
        "rows": len(frame),
        "columns": frame.shape[1] - 1,
        "hours": round(float(frame["ts_ms"].iloc[-1] - frame["ts_ms"].iloc[0]) / 3.6e6, 1),
        "cycles": int(cycle_id.max()),
        "labels": labels.value_counts().to_dict(),
    }
