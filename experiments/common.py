"""Shared helpers for the Try this experiments: run stages 3 and 4 (cycle processing and the
process-aware diagnosis) on a table of samples and score the result against the labels."""

import copy
import sys
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from ff_pipeline import data, rca  # noqa: E402
from ff_pipeline.processor import CycleProcessor, CycleSummary  # noqa: E402

CONFIG = data.load_config()
TEMPLATE = rca.load_template(ROOT / CONFIG["template"]["path"])
TOLERANCE = CONFIG["template"]["match_tolerance_s"]


def summaries(frame, config: Optional[Dict] = None) -> List[CycleSummary]:
    """Stream the rows through the cycle processor, as the pipeline does, and keep the complete cycles."""
    processor = CycleProcessor(config or CONFIG)
    closed = [s for e in data.events(frame) if (s := processor.process(e))]
    closed.append(processor.flush())
    return [s for s in closed if s is not None and s.complete]


def score(cycles: List[CycleSummary], labels: Dict[int, str]) -> Dict:
    diagnoses = [rca.diagnose(s, TEMPLATE, TOLERANCE) for s in cycles]
    result = rca.evaluate(diagnoses, labels) if diagnoses else {"cycles": 0}
    result["grasps"] = sum(len(s.grasps) for s in cycles)
    return result


def config_with(**changes) -> Dict:
    """A copy of config/pipeline.json with some grasp settings changed."""
    config = copy.deepcopy(CONFIG)
    config["grasp"].update(changes)
    return config


def fmt(value) -> str:
    return "-" if value is None else f"{value:.2f}" if isinstance(value, float) else str(value)
