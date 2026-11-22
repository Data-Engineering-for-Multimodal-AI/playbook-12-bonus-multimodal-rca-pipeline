"""Checks for the "Use your own data" loader (scripts/own_data.py). No downloads, no services."""
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from own_data import scan  # noqa: E402

SAMPLE = pd.read_parquet(REPO / "data" / "sample" / "ff_sample.parquet")
R03 = [c for c in SAMPLE.columns if "_R03_" in c]


def kaggle_layout(folder: Path, frame: pd.DataFrame = SAMPLE) -> None:
    """Two CSVs that share the _time column, as in the Kaggle export."""
    folder.mkdir(exist_ok=True)
    times = pd.to_datetime(frame["ts_ms"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    frame[R03].assign(_time=times).to_csv(folder / "R03_Data.csv", index=False)
    frame[["Description", "Q_Cell_CycleCount"]].assign(_time=times).to_csv(folder / "FFCell_CycleManagement.csv", index=False)


def test_bundled_sample_passes(tmp_path):
    SAMPLE.to_parquet(tmp_path / "line.parquet")
    table, problems, notes = scan(tmp_path)
    assert problems == []
    assert len(table) == len(SAMPLE)
    assert "12 complete cycles" in notes[0]


def test_csv_files_are_joined_on_time(tmp_path):
    kaggle_layout(tmp_path)
    (tmp_path / ".DS_Store").write_bytes(b"\0")
    table, problems, _ = scan(tmp_path)
    assert problems == []
    assert table["ts_ms"].tolist() == SAMPLE["ts_ms"].tolist()
    assert set(R03) | {"Description", "Q_Cell_CycleCount"} <= set(table.columns)
    assert (table["Description"] == SAMPLE["Description"]).all()


def test_no_labels_is_a_note(tmp_path):
    SAMPLE.drop(columns=["Description"]).to_parquet(tmp_path / "line.parquet")
    table, problems, notes = scan(tmp_path)
    assert problems == [] and (table["Description"] == "").all()
    assert any("no Description column" in n for n in notes)


@pytest.mark.parametrize("name,content,expected", [
    ("notes.txt", b"hello", "not a .csv or .parquet file"),
    ("empty.csv", b"", "empty file"),
    ("R01_Data.csv", b"version https://git-lfs.github.com/spec/v1\noid sha256:0\n", "Git LFS pointer"),
    ("broken.parquet", b"not parquet", "can't read as parquet"),
    ("latin1.csv", b"_time,x\n2023-12-11T13:00:00Z,\xe9\n", "not UTF-8 text"),
    ("notime.csv", b"time,x\n1,2\n", "no time column"),
    ("badtime.csv", b"_time,x\nyesterday,2\n", "isn't an ISO 8601 timestamp"),
    ("seconds.csv", b"ts_ms,x\n1702314024,2\n", "outside 2000-2100"),
    ("unsorted.csv", b"ts_ms,x\n1702314024942,1\n1702314024842,2\n", "go back in time at row 3"),
    ("repeated.csv", b"ts_ms,x\n1702314024942,1\n1702314024942,2\n", "repeated timestamps"),
])
def test_bad_files_are_reported(tmp_path, name, content, expected):
    kaggle_layout(tmp_path)
    (tmp_path / name).write_bytes(content)
    table, problems, _ = scan(tmp_path)
    assert table is None
    assert len(problems) == 1 and problems[0].startswith(name) and expected in problems[0], problems


def test_every_bad_file_is_listed(tmp_path):
    kaggle_layout(tmp_path)
    (tmp_path / "a.txt").write_text("x")
    (tmp_path / "b.csv").write_text("")
    _, problems, _ = scan(tmp_path)
    assert [p.split(":")[0] for p in problems] == ["a.txt", "b.csv"]


def test_table_checks(tmp_path):
    frame = SAMPLE.copy()
    frame.loc[100:104, "I_R03_Gripper_Load"] = None
    frame.loc[200, "Description"] = "NoBase"
    frame.to_parquet(tmp_path / "line.parquet")
    _, problems, _ = scan(tmp_path)
    assert problems[0] == "I_R03_Gripper_Load: 5 samples missing or not a number (first at 2023-12-11 17:00:34 UTC)"
    assert len(problems) == 2 and "unknown labels 'NoBase'" in problems[1]


def test_missing_column_slow_rate_and_short_cycles(tmp_path):
    SAMPLE.drop(columns=["I_R03_Gripper_Load"]).to_parquet(tmp_path / "line.parquet")
    assert scan(tmp_path)[1] == ["no file has the column I_R03_Gripper_Load"]
    SAMPLE.iloc[::30].to_parquet(tmp_path / "line.parquet")          # one sample every 3 s
    assert "R03's grasps need at least 1 sample per second" in scan(tmp_path)[1][0]
    SAMPLE.iloc[:1000].to_parquet(tmp_path / "line.parquet")         # 100 s, less than one cycle
    assert "no complete cycle" in scan(tmp_path)[1][0]


def test_not_a_folder(tmp_path):
    assert scan(tmp_path / "nope")[1] == [f"{tmp_path / 'nope'}: not a folder"]
