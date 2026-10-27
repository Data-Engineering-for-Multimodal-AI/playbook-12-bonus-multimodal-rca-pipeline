"""Smoke tests on the bundled sample (a few seconds, no downloads, no services)."""
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import download_data  # noqa: E402
from ff_pipeline import data, rca  # noqa: E402
from ff_pipeline.pipeline import run  # noqa: E402
from ff_pipeline.processor import CycleProcessor, CycleSummary, Grasp  # noqa: E402

CONFIG = data.load_config()
TEMPLATE = rca.load_template()


def summary(starts, complete=True):
    return CycleSummary(1, 0, 295_000, 2950, [Grasp(t, 6.0, 9000.0) for t in starts], {}, complete)


def test_processor_finds_grasps_and_cycle_boundaries():
    events = []
    for cycle, start in ((1, 0), (2, 300_000), (2, 900_000)):   # the third block follows a 10-minute gap
        for i in range(2500):
            load = 8000.0 if 1000 <= i < 1060 else 1370.0          # one grasp, 6 s, at 100 s
            events.append({"ts_ms": start + i * 100, "Q_Cell_CycleCount": cycle, "I_R03_Gripper_Load": load})
    processor = CycleProcessor(CONFIG)
    closed = [s for e in events if (s := processor.process(e))] + [processor.flush()]
    assert len(closed) == 3 and all(s.complete for s in closed)
    assert [(g.start_s, g.duration_s) for g in closed[0].grasps] == [(100.0, 6.0)]


def test_rca_names_missing_pieces_and_allows_for_late_cycles():
    full = TEMPLATE["start_s"]
    assert rca.diagnose(summary(full), TEMPLATE, 3)["missing"] == []
    nose = TEMPLATE["start_s"][TEMPLATE["pieces"].index("nose")]
    without_nose = [t for t in full if t != nose]
    assert rca.diagnose(summary(without_nose), TEMPLATE, 3)["missing"] == ["nose"]
    late = [t + 20 for t in without_nose]                         # the whole cycle runs 20 s late
    assert rca.diagnose(summary(late), TEMPLATE, 3)["missing"] == ["nose"]
    one_late = full[:-1] + [full[-1] + 10]                       # all four grasps, the last one late
    result = rca.diagnose(summary(one_late), TEMPLATE, 3)
    assert not result["defect"] and "warning" in result


def test_commissioning_names_slots_from_known_faults():
    slots = {"start_s": [10.0, 20.0, 30.0, 40.0]}
    faulty = {"NoNose": [summary([10, 20, 40])] * 3,
              "NoNose,NoBody2": [summary([10, 40])] * 3,
              "NoNose,NoBody2,NoBody1": [summary([40])] * 3}
    named = rca.commission(slots, faulty, 3, ["base", "body1", "body2", "nose"])
    assert named["pieces"] == ["body1", "body2", "nose", "base"]


def test_download_rejects_a_changed_file(tmp_path):
    archive = tmp_path / "ff.zip"
    with zipfile.ZipFile(archive, "w") as z:
        for name in download_data.FILES:
            z.writestr(f"Analog Dataset/{name}", "_time,Description\n")
    with pytest.raises(SystemExit, match="SHA-256 mismatch"):
        download_data.verify(archive)


def test_sample_end_to_end(tmp_path):
    report = run("sample", output=str(tmp_path))
    steps = report["steps"]
    assert steps["Ingest"]["cycles"] == 12 and steps["Process"]["complete_cycles"] == 12
    scored = steps["Lake and analytics"]["process_aware_rca"]
    assert scored["precision"] == 1.0 and scored["recall"] == 1.0 and scored["missing_pieces_exact"] == 1.0
    # The generic detector flags the complete rockets: sensor drift, not defects
    assert steps["Lake and analytics"]["generic_detector"]["false_positives"] == 3
    assert steps["Edge"]["reduction_percent"] > 99
    assert (tmp_path / "report.json").exists() and (tmp_path / "dashboard.txt").exists()
    gold = pd.read_parquet(tmp_path / "lake" / "gold" / "hourly_quality.parquet")
    assert gold["defects"].sum() == 9
    assert list((tmp_path / "lake" / "bronze").glob("date=*/hour=*/*.parquet"))
