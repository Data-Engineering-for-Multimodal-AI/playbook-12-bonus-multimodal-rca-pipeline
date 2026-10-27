#!/usr/bin/env python3
"""
Download the Future Factories analog dataset from Kaggle and convert it for the pipeline.

    python download_data.py                # -> data/ff-analog/ff_analog.parquet (about 30 MB)
    python download_data.py --make-sample  # also rebuild the bundled sample (maintainers)

The dataset (CC BY-SA 4.0) is 30 hours of a rocket assembly line at the University of South
Carolina's Future Factories lab: four robots and a conveyor sampled at 10 Hz, with cycles where team
members removed pieces from the tray. Kaggle needs an account: put your API token in
~/.kaggle/kaggle.json, or set KAGGLE_USERNAME and KAGGLE_KEY (kaggle.com > Settings > API).
"""

import argparse
import hashlib
import json
import sys
import zipfile
from pathlib import Path

import pandas as pd

DATASET = "ramyharik/ff-2023-12-12-analog-dataset"
# SHA-256 of each CSV in the Kaggle zip (the zip itself may be rebuilt by Kaggle)
FILES = {
    "Conveyor_Signals.csv": "e69dc580afa6c6d4659a2070c34a1b49888a23de040a53d4403df4059bdd38fb",
    "FFCell_CycleManagement.csv": "d9af2e4e91c20559648d033c3df2fd8a2ad3aee2360ccf9d66b1bd280661b112",
    "FFCell_SafetyManagement.csv": "14e06c025f08362cf23fb4f0e47368ffaf07d08c4eb0d2e278c0f75b0ac300e2",
    "R01_Data.csv": "8578ed525699d0f7aea6ec02075056245fd6dd693fd893948a966678405f77c5",
    "R02_Data.csv": "a66795adbdad9d988eecfdea3d9f3a750a0baa91e07148d01c5d27418c746eba",
    "R03_Data.csv": "fd35ae0a8c97ed083e0d2c821c5b0169e0bbaed92d2dedcb5dd2e548de7f23a0",
    "R04_Data.csv": "21e85105827b287beaa12fd03625587d03fdde1d6977b2122427e84af895a0e6",
}
ROOT = Path(__file__).resolve().parent
OUT = ROOT / "data" / "ff-analog"
SAMPLE = ROOT / "data" / "sample" / "ff_sample.parquet"


def verify(zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path) as z:
        names = {Path(n).name: n for n in z.namelist()}
        for name, expected in FILES.items():
            if name not in names:
                sys.exit(f"{name} is missing from {zip_path.name}")
            digest = hashlib.sha256()
            with z.open(names[name]) as f:
                while chunk := f.read(1 << 20):
                    digest.update(chunk)
            if digest.hexdigest() != expected:
                sys.exit(f"SHA-256 mismatch for {name}: Kaggle may have published a new version of the dataset")


def download(cache: Path) -> Path:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        sys.exit("The kaggle package is missing: pip install -r requirements.txt")
    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as e:  # missing or invalid token
        sys.exit(f"Kaggle login failed ({e}). Put your API token in ~/.kaggle/kaggle.json "
                 "or set KAGGLE_USERNAME and KAGGLE_KEY.")
    cache.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {DATASET} from Kaggle (61 MB)...")
    api.dataset_download_files(DATASET, path=str(cache), quiet=True)
    return cache / (DATASET.split("/")[1] + ".zip")


def convert(zip_path: Path) -> pd.DataFrame:
    """The seven CSVs share one time column, row for row; join them into one table."""
    frames = []
    with zipfile.ZipFile(zip_path) as z:
        names = {Path(n).name: n for n in z.namelist()}
        for name in FILES:
            with z.open(names[name]) as f:
                df = pd.read_csv(f, low_memory=False)
            if frames:
                if not df["_time"].equals(frames[0]["_time"]):
                    sys.exit(f"{name}: timestamps differ from {FILES[0]}")
                df = df.drop(columns=["_time", "Description"])
            frames.append(df)
    data = pd.concat(frames, axis=1)
    times = pd.to_datetime(data.pop("_time"), format="ISO8601")
    data.insert(0, "ts_ms", (times - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1))
    for column in data.columns:
        if data[column].dtype == object and column != "Description":
            # TRUE/FALSE flags, some with gaps
            data[column] = data[column].map({"TRUE": True, "FALSE": False, True: True, False: False}).astype("boolean")
    # The label: which pieces team members removed from the tray in that cycle (empty = complete rocket)
    data["Description"] = data["Description"].fillna("")
    return data


def make_sample(data: pd.DataFrame) -> pd.DataFrame:
    """Three consecutive cycles of each label, 12 cycles, about 35,000 rows. The complete rockets come
    from the second block of complete cycles, 15 hours into the run, so the sample shows sensor drift."""
    from ff_pipeline.data import cycle_ids
    cycle_id = cycle_ids(data)
    labels = data.groupby(cycle_id)["Description"].first()
    sizes = data.groupby(cycle_id).size()
    picked = []
    for label in ["", "NoNose", "NoNose,NoBody2", "NoNose,NoBody2,NoBody1"]:
        complete = [c for c in labels.index if labels[c] == label and sizes[c] > 2500]
        picked += complete[100:103] if label == "" else complete[3:6]   # not the first cycles of a block
    return data[cycle_id.isin(picked)].reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--zip", type=Path, help="use an already downloaded ff-2023-12-12-analog-dataset.zip")
    parser.add_argument("--make-sample", action="store_true", help="also rebuild data/sample/ff_sample.parquet")
    args = parser.parse_args()

    zip_path = args.zip or download(OUT / "download")
    verify(zip_path)
    print("  SHA-256 of the 7 CSV files verified")
    data = convert(zip_path)
    OUT.mkdir(parents=True, exist_ok=True)
    data.to_parquet(OUT / "ff_analog.parquet", index=False)
    span = (data["ts_ms"].iloc[-1] - data["ts_ms"].iloc[0]) / 3.6e6
    print(f"  {len(data):,} rows x {data.shape[1]} columns, {span:.1f} hours -> "
          f"{(OUT / 'ff_analog.parquet').relative_to(ROOT)} "
          f"({(OUT / 'ff_analog.parquet').stat().st_size / 1e6:.0f} MB)")
    with open(OUT / "SOURCE.json", "w") as f:
        json.dump({"dataset": f"https://www.kaggle.com/datasets/{DATASET}", "csv_sha256": FILES,
                   "license": "CC BY-SA 4.0", "paper": "https://arxiv.org/abs/2401.15544"}, f, indent=2)

    if args.make_sample:
        sample = make_sample(data)
        SAMPLE.parent.mkdir(parents=True, exist_ok=True)
        sample.to_parquet(SAMPLE, index=False)
        print(f"  Sample: {len(sample):,} rows -> {SAMPLE.relative_to(ROOT)} ({SAMPLE.stat().st_size / 1e6:.1f} MB)")
    print("Next: python demo_pipeline.py --data full")
    return 0


if __name__ == "__main__":
    sys.exit(main())
