#!/usr/bin/env python3
"""
Use your own line data: check a folder of recordings, then run the pipeline on it.

    python scripts/own_data.py --dir my_line --check-only   # check every file, write nothing
    python scripts/own_data.py --dir my_line                # check, join, run the pipeline

The folder holds .csv or .parquet files in the Future Factories layout: one row per sample, a time
column (`ts_ms`, milliseconds since 1970 UTC, or `_time`, ISO 8601 text as in the Kaggle CSVs), the
cell's cycle counter `Q_Cell_CycleCount`, R03's gripper load `I_R03_Gripper_Load`, and optionally the
label `Description` and any other sensor columns. Files that share the time column (the Kaggle export
has one file per robot) are joined on it. Output goes to output/own/.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ff_pipeline import data  # noqa: E402

SUFFIXES = {".csv", ".parquet"}
REQUIRED = ["Q_Cell_CycleCount", "I_R03_Gripper_Load"]
LABEL_RE = re.compile(r"^((NoNose|NoBody1|NoBody2)(,(NoNose|NoBody1|NoBody2))*)?$")
MAX_BYTES = 2_000_000_000          # the whole table is loaded into memory
MAX_INTERVAL_MS = 1100             # grasps need about 1 Hz or faster (README, Try this 1)
OUT = ROOT / "output" / "own"


def _read(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def _times(frame: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str]]:
    """The time column as milliseconds since 1970 UTC, or a reason it can't be used."""
    if "ts_ms" in frame.columns:
        ts = pd.to_numeric(frame["ts_ms"], errors="coerce")
        if ts.isna().any():
            return None, f"{int(ts.isna().sum())} rows with a ts_ms that isn't a number (first at row {int(ts.isna().idxmax()) + 2})"
        if (ts < 946_684_800_000).any() or (ts > 4_102_444_800_000).any():
            return None, "ts_ms values outside 2000-2100: ts_ms must be milliseconds since 1970 UTC"
        return ts.astype("int64"), None
    if "_time" in frame.columns:
        parsed = pd.to_datetime(frame["_time"], format="ISO8601", errors="coerce", utc=True)
        if parsed.isna().any():
            return None, f"{int(parsed.isna().sum())} rows with a _time that isn't an ISO 8601 timestamp (first at row {int(parsed.isna().idxmax()) + 2})"
        return (parsed - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1), None
    return None, "no time column (ts_ms or _time)"


def scan(folder: Path) -> Tuple[Optional[pd.DataFrame], List[str], List[str]]:
    """Check every file and the joined table. Returns (table or None, problems, notes)."""
    folder = Path(folder)
    problems: List[str] = []
    notes: List[str] = []
    if not folder.is_dir():
        return None, [f"{folder}: not a folder"], notes
    frames: List[pd.DataFrame] = []
    files = sorted(p for p in folder.rglob("*") if p.is_file() and not p.name.startswith("."))
    for path in files:
        name = str(path.relative_to(folder))
        if path.suffix.lower() not in SUFFIXES:
            problems.append(f"{name}: not a .csv or .parquet file")
            continue
        size = path.stat().st_size
        if size == 0:
            problems.append(f"{name}: empty file")
            continue
        if size > MAX_BYTES:
            problems.append(f"{name}: {size / 1e9:.1f} GB, more than the 2 GB this loader reads into memory; split it by time")
            continue
        with open(path, "rb") as f:
            if f.read(100).startswith(b"version https://git-lfs"):
                problems.append(f"{name}: a Git LFS pointer, not the data (run git lfs pull)")
                continue
        try:
            frame = _read(path)
        except UnicodeDecodeError:
            problems.append(f"{name}: not UTF-8 text")
            continue
        except Exception as e:  # noqa: BLE001 - any reader error is a reason to skip the file
            problems.append(f"{name}: can't read as {path.suffix[1:]} ({type(e).__name__}: {str(e).splitlines()[0][:80]})")
            continue
        if frame.empty:
            problems.append(f"{name}: no rows")
            continue
        ts, reason = _times(frame)
        if reason:
            problems.append(f"{name}: {reason}")
            continue
        if ts.duplicated().any():
            problems.append(f"{name}: {int(ts.duplicated().sum())} repeated timestamps (first {_utc(ts[ts.duplicated()].iloc[0])})")
            continue
        if not ts.is_monotonic_increasing:
            back = int((ts.diff() < 0).idxmax())
            problems.append(f"{name}: timestamps go back in time at row {back + 2}; sort the file by time")
            continue
        frame = frame.drop(columns=[c for c in ("ts_ms", "_time") if c in frame.columns])
        frame.insert(0, "ts_ms", ts.to_numpy())
        frames.append(frame)
    if not files:
        problems.append(f"{folder}: no files")
    if problems or not frames:
        return None, problems, notes

    table = _join(frames)
    missing = [c for c in REQUIRED if c not in table.columns]
    if missing:
        return None, [f"no file has the column{'s' if len(missing) > 1 else ''} {', '.join(missing)}"], notes
    for column in REQUIRED:
        values = pd.to_numeric(table[column], errors="coerce")
        if values.isna().any():
            first = _utc(table["ts_ms"][values.isna()].iloc[0])
            problems.append(f"{column}: {int(values.isna().sum())} samples missing or not a number (first at {first})")
        table[column] = values
    if (table["Q_Cell_CycleCount"].dropna() % 1 != 0).any():
        problems.append("Q_Cell_CycleCount: not a whole number in every row; it must be the cell's cycle counter")
    interval = float(table["ts_ms"].diff().median())
    if interval > MAX_INTERVAL_MS:
        problems.append(f"samples are {interval / 1000:.1f} s apart (median); R03's grasps need at least 1 sample per second")
    if "Description" in table.columns:
        labels = table["Description"].fillna("").astype(str).str.strip()
        bad = sorted(set(labels[~labels.str.match(LABEL_RE)]))
        if bad:
            problems.append(f"Description: unknown labels {', '.join(map(repr, bad[:5]))}; use empty (complete) or "
                            "NoNose, NoBody1, NoBody2 separated by commas")
        table["Description"] = labels
    else:
        notes.append("no Description column: every cycle is scored as a complete rocket")
        table["Description"] = ""
    if problems:
        return None, problems, notes
    table["Q_Cell_CycleCount"] = table["Q_Cell_CycleCount"].astype("int64")
    for column in table.columns:
        if table[column].dtype == object or str(table[column].dtype) == "str":
            if column != "Description":
                # TRUE/FALSE flags, as in the Kaggle CSVs
                table[column] = table[column].map({"TRUE": True, "FALSE": False, True: True, False: False}).astype("boolean")
    ids = data.cycle_ids(table)
    durations = table.groupby(ids)["ts_ms"].agg(lambda s: (s.iloc[-1] - s.iloc[0]) / 1000)
    complete = int((durations >= data.load_config()["cycle"]["min_seconds"]).sum())
    if complete == 0:
        problems.append(f"no complete cycle: the longest is {durations.max():.0f} s, a cycle needs at least "
                        f"{data.load_config()['cycle']['min_seconds']} s")
        return None, problems, notes
    notes.insert(0, f"{len(table):,} samples, {len(table.columns) - 2} signals, "
                    f"{(table['ts_ms'].iloc[-1] - table['ts_ms'].iloc[0]) / 3.6e6:.1f} h, {complete} complete cycles")
    return table, problems, notes


def _join(frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Files with the same timestamps are joined side by side; files with different times are stacked."""
    joined = pd.concat(frames, ignore_index=True, sort=False)
    joined = joined.groupby("ts_ms", sort=True).first().reset_index()
    return joined


def _utc(ts) -> str:
    return pd.Timestamp(int(ts), unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M:%S UTC")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", required=True, type=Path, help="folder with your .csv or .parquet files")
    parser.add_argument("--check-only", action="store_true", help="check the files and write nothing")
    args = parser.parse_args(argv)
    table, problems, notes = scan(args.dir)
    if problems:
        print(f"{len(problems)} problem{'s' if len(problems) > 1 else ''} in {args.dir}; nothing was written:")
        for p in problems:
            print(f"  {p}")
        return 1
    print(f"{args.dir}: OK")
    for n in notes:
        print(f"  {n}")
    if args.check_only:
        return 0
    from ff_pipeline.pipeline import print_step, run
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "ff_own.parquet"
    table.to_parquet(path, index=False)
    report = run(str(path), output=str(OUT))
    print_step(report, 6)
    return 0


if __name__ == "__main__":
    sys.exit(main())
