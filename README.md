# Streaming Root Cause Analysis on a Robot Assembly Line

**Bonus content for Chapter 12** of *Data Engineering for Multimodal AI* (O'Reilly, 2026). It is not printed in the book; it applies the [Chapter 12 playbook](https://github.com/Data-Engineering-for-Multimodal-AI/playbook-12-multimodal-streamkit-ops-pipeline)'s pattern (ingest, fuse, plan, check, act, observe) to a factory.
Book: https://oreil.ly/data-engineering-multimodal

> Want the repository as it was when the book was published? Use the `first-edition` tag. This branch is the 2026 maintenance update; see [Differences from the first edition](#differences-from-the-first-edition).

## What you'll build

A streaming pipeline that finds defective products on a real assembly line, and says why.

The data is 29 hours of the Future Factories testbed at the University of South Carolina: four robots and a conveyor build a small rocket from four pieces (base, two body pieces, nose cone), take it apart and start again, 325 times. Every robot, the conveyor drives and the cell's safety and cycle signals were recorded at 10 Hz (974,836 samples of 48 signals). In 164 of the cycles, the team removed pieces from the tray before assembly; the dataset labels which ones.

The pipeline:

1. **Ingest** the recording (a 12-cycle sample is bundled; the full 29 hours download from Kaggle).
2. **Stream** the samples as events, through an in-process queue or a Kafka broker.
3. **Process** the stream: close each assembly cycle and summarize it, including when robot R03, which builds the rocket, actually held a piece (its gripper's load cell).
4. **Diagnose at the edge**: compare each cycle's grasps with a template of a complete cycle; a grasp that isn't there means R03 reached for a piece and found nothing. Only the cycle summaries and diagnoses go upstream, not the raw samples.
5. **Store and analyze**: a bronze/silver/gold Parquet lake (raw events by hour, one row per cycle, quality per hour), and a score of every diagnosis against the labels, next to a generic anomaly detector that knows nothing about the process.
6. **Observe**: a JSON report and a text dashboard.

What it shows:

- **Knowing the process beats generic anomaly detection.** The process-aware check finds every defective cycle and names the missing pieces exactly. A z-score detector over all 45 sensor signals also catches every defect, but flags 144 of the 161 complete rockets as well: one robot's joint-angle reading drifts over the day, and the detector can't tell drift from defects.
- **Where to look matters more than how much data.** The removed pieces don't show in the conveyor, in R01 (which carries the tray) or in R02 (which handles the body pieces): their grippers close the same way with or without a piece. Only R03's load cell tells whether it gripped something.
- **The edge decides what travels.** Forwarding one summary per cycle instead of every sample cuts the data sent upstream by 99.96%.

## Quick start

You need Python 3.12 or newer.

```bash
git clone https://github.com/Data-Engineering-for-Multimodal-AI/playbook-12-bonus-multimodal-rca-pipeline.git
cd playbook-12-bonus-multimodal-rca-pipeline

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

python demo_pipeline.py --data sample       # the bundled 12 cycles: a few seconds
```

With [uv](https://docs.astral.sh/uv/): `uv sync`, then prefix commands with `uv run`. The Makefile has shortcuts: `make run`, `make run-full`, `make run-kafka`, `make smoke`.

`python cloud_demo.py --step N` shows one step's output (1 Ingest ... 6 Observe); the pipeline runs once either way. Output goes to `output/sample/` or `output/full/`: `report.json`, `dashboard.txt` and the `lake/` folders.

### The full 29 hours

The dataset is on Kaggle, which needs a free account and an API token (kaggle.com > Settings > API > Create New Token; save it as `~/.kaggle/kaggle.json`, or set `KAGGLE_USERNAME` and `KAGGLE_KEY`, see `.env.example`):

```bash
python download_data.py              # 61 MB download -> data/ff-analog/ff_analog.parquet (26 MB)
python demo_pipeline.py --data full  # about a minute
```

`download_data.py` checks the SHA-256 of the seven CSV files in the download and joins them into one table.

### Through Kafka (optional)

```bash
docker compose up -d --wait kafka                    # one broker, KRaft mode, on 127.0.0.1:9092
python demo_pipeline.py --data sample --transport kafka
docker compose down
```

A producer thread publishes every sample to a new topic as a JSON message while the pipeline consumes it. `--speed 10` replays at 10 times real time instead of as fast as possible.

### How the diagnosis works

`config/r03_template.json` holds the template:

- **When R03 grips**: the median grasp times of the first 10 cycles of the recording, which are all complete rockets. No labels are used: about 66, 79, 92 and 103 s into a cycle.
- **Which piece each grasp is**: commissioning, as an engineer would do it by removing each piece once. The first 3 cycles of each kind of fault show which grasp disappears: the 92 s grasp is the nose cone, 79 s is body2 and 66 s is body1. The 103 s grasp never disappears; it is the base, the one piece never removed. These 9 cycles are listed in the template and reported separately in the scores.

A cycle's grasps are matched to the template in order, allowing for the whole cycle running late (some run 7 to 20 s late). Grasps more than 3 s off once aligned are reported as a timing warning, not a defect. `make learn-template` rebuilds the template and the generic detector's baseline from the downloaded data.

## Tested on

| Date | OS | Python | What |
|---|---|---|---|
| 2026-10-27 | macOS 26.5.1 (Apple M3) | 3.13.16 | `make smoke` (5 tests); the sample and the full 29 hours, in memory and through Kafka 4.4.0 (Docker) |
| 2026-10-27 | macOS 26.5.1 (Apple M3) | 3.12.15 | Fresh clone: `pip install -r requirements.txt` (14 s), the sample, `python download_data.py` from Kaggle (12 s) and the full 29 hours (40 s, 24,443 events/s; same results) |
| 2026-10-27 | Ubuntu (GitHub Actions) | 3.12, 3.13 | `make smoke`; `pip install -r requirements.txt`, then the scripts on the sample; the sample through Kafka |

Versions are pinned in `uv.lock` and `requirements.txt` (NumPy 2.5, pandas 3.1, PyArrow 26, confluent-kafka 2.16, kaggle 2.2). The full dataset needs a Kaggle token, so CI runs the sample only.

## Differences from the first edition

The book doesn't print this repository. The first edition (tag `first-edition`) described a manufacturing streaming platform but didn't run:

| First edition | Now |
|---|---|
| Four of the five Python files didn't compile: their indentation had been stripped | One package, `ff_pipeline/`, with tests and CI |
| `cloud_demo.py` printed hard-coded results ("Successfully ingested 287954 records") with pauses in between; it read no data | `cloud_demo.py --step N` runs the pipeline and shows that step |
| The README's results (15,000 events/s, 99.97% uptime, under 10 ms edge inference, 75% bandwidth reduction) weren't measured | Every number below comes from a run, with the date and machine |
| Data read from a Cloud Storage bucket that isn't public | Downloaded from Kaggle, where the dataset's authors publish it; a 12-cycle sample is bundled |
| `docker-compose.yml` wasn't valid YAML and mounted configuration files that weren't in the repository (Kafka with ZooKeeper, Flink, Spark, Airflow, Grafana and more) | One Kafka broker in KRaft mode, for the optional Kafka transport |
| `requirements.txt`: 107 unpinned packages, most never imported | The 5 packages the code uses, pinned |
| No root cause analysis, despite the repository's name | Missing pieces diagnosed per cycle and scored against the labels |
| Apache Flink, PyFlink, MQTT, Redis, ONNX, BigQuery, NVIDIA Jetson and TensorRT described | Not used. The stream processing is plain Python, so it runs on a laptop without Java; Kafka is optional |
| `edge/edge_inference.py`, `streaming/flink_processor.py`, `streaming/gcp_kafka_producer.py`, `TUTORIAL.md` | Removed (they're in the `first-edition` tag); `demo_pipeline.py` and `cloud_demo.py` remain |
| ACKNOWLEDGMENTS listed hardware, cloud and academic partners | Only the dataset's authors and the funding their paper states |

## Data and licenses

| Data | Source | License |
|---|---|---|
| `data/sample/ff_sample.parquet` (bundled, 1.5 MB): 12 cycles, three of each label | Adapted from the dataset below by `download_data.py --make-sample` | **CC BY-SA 4.0**, as the source requires; not covered by the repository's MIT license. See `data/sample/README.md` |
| `data/ff-analog/` (downloaded): 974,836 samples, 2023-12-11 13:04 to 2023-12-12 18:07 UTC | R. Harik, F. El Kalach, J. Samaha, D. Clark, D. Sander, P. Samaha, L. Burns, I. Yousif, V. Gadow, T. Tarekegne, N. Saha, "Analog and Multi-modal Manufacturing Datasets Acquired on the Future Factories Platform", [arXiv:2401.15544](https://arxiv.org/abs/2401.15544). [Kaggle: ramyharik/ff-2023-12-12-analog-dataset](https://www.kaggle.com/datasets/ramyharik/ff-2023-12-12-analog-dataset) | CC BY-SA 4.0. Not redistributed beyond the sample |
| `config/r03_template.json`, `config/generic_baseline.json` | Learned from the dataset (first 10 cycles, plus 9 labelled cycles for the piece names) | CC BY-SA 4.0 (derived from the dataset) |

The dataset also has a multimodal part with camera images (6 downloads, the first alone 104 GB); this pipeline uses the sensor data only. The code is MIT-licensed (see [LICENSE](LICENSE)).

## Expected output

### The bundled sample

From `python demo_pipeline.py --data sample` on 2026-10-27, macOS 26.5.1 (Apple M3, 8 GB), Python 3.13.16, with other applications running. Speeds depend on the machine and its load.

```
STEP 2: STREAM
------------------------------------------------------------------------
{
  "transport": "memory",
  "events": 36166,
  "seconds": 1.6,
  "events_per_second": 22154,
  "replay_speed": "as fast as possible"
}

STEP 3: PROCESS
------------------------------------------------------------------------
{
  "cycles_closed": 12,
  "complete_cycles": 12,
  "incomplete_segments": 0,
  "r03_grasps_per_cycle": {
    "4": 3,
    "3": 3,
    "2": 3,
    "1": 3
  },
  "close_and_diagnose_ms_p50": 0.077,
  "close_and_diagnose_ms_max": 0.11
}

STEP 4: EDGE
------------------------------------------------------------------------
{
  "raw_events": 36166,
  "raw_mb": 59.6,
  "forwarded_messages": 12,
  "forwarded_mb": 0.025,
  "reduction_percent": 99.96,
  "alerts": 9,
  "warnings": 1
}

STEP 6: OBSERVE
------------------------------------------------------------------------
========================================================================
FUTURE FACTORIES STREAMING RCA: DASHBOARD
========================================================================
Data:      data/sample/ff_sample.parquet: 36,166 events, 18.4 h, 12 complete cycles
Stream:    memory, 22,154 events/s (1.6 s)
Edge:      59.6 MB of raw events -> 0.025 MB forwarded (99.96% less); 9 defect alerts, 1 timing warnings

Root cause analysis against the labels (cycles scored, excluding the commissioning cycles):
  Process-aware (R03 grasps):  precision 1.0, recall 1.0, missing pieces exactly right 1.0 (12 cycles)
  Generic (sensor z-scores):   precision 0.75, recall 1.0 (3 complete rockets flagged; top sensor M_R01_UJointAngle_Degree)

Label -> diagnosis (all cycles):
  NoNose                   nose: 3
  NoNose,NoBody2           body2,nose: 3
  NoNose,NoBody2,NoBody1   body1,body2,nose: 3
  complete                 complete: 3

Examples:
  cycle 3 (NoNose): nose missing from the tray: R03 found nothing to grip at about 92 s into the cycle
  cycle 13 (NoNose,NoBody2): body2, nose missing from the tray: R03 found nothing to grip at about 79 s into the cycle
  cycle 64 (NoNose,NoBody2,NoBody1): body1, body2, nose missing from the tray: R03 found nothing to grip at about 66 s into the cycle
  cycle 192 (complete): complete rocket
  cycle 194 (complete): complete, but the base grasp came +12.2 s off schedule
========================================================================

Written: output/sample/report.json, output/sample/dashboard.txt
```

The sample's complete rockets come from 15 hours into the run, so the generic detector flags them: the drift, not a defect. Cycle 194 is complete, but its base grasp came 12 s late, so it gets a warning instead of an alert.

### The full 29 hours

From `python demo_pipeline.py --data full`, same machine and date:

```
========================================================================
FUTURE FACTORIES STREAMING RCA: DASHBOARD
========================================================================
Data:      data/ff-analog/ff_analog.parquet: 974,836 events, 29.1 h, 325 complete cycles
Stream:    memory, 18,422 events/s (52.9 s)
Edge:      1603.0 MB of raw events -> 0.678 MB forwarded (99.96% less); 164 defect alerts, 6 timing warnings

Root cause analysis against the labels (cycles scored, excluding the commissioning cycles):
  Process-aware (R03 grasps):  precision 1.0, recall 1.0, missing pieces exactly right 1.0 (316 cycles)
  Generic (sensor z-scores):   precision 0.532, recall 1.0 (144 complete rockets flagged; top sensor M_R01_UJointAngle_Degree)

Label -> diagnosis (all cycles):
  complete                 complete: 161
  NoNose                   nose: 59
  NoNose,NoBody2           nose: 1, body2,nose: 50
  NoNose,NoBody2,NoBody1   body1,body2,nose: 54

Cycles where the diagnosis differs from the label:
  cycle 10 at 2023-12-11 19:59:56 UTC: label NoNose,NoBody2, diagnosis nose (commissioning cycle)

Examples:
  cycle 1 (complete): complete rocket
  cycle 5 (complete): complete, but the base grasp came +4.2 s off schedule
  cycle 15 (NoNose): nose missing from the tray: R03 found nothing to grip at about 98 s into the cycle
  cycle 11 (NoNose,NoBody2): body2, nose missing from the tray: R03 found nothing to grip at about 86 s into the cycle
  cycle 61 (NoNose,NoBody2,NoBody1): body1, body2, nose missing from the tray: R03 found nothing to grip at about 72 s into the cycle
========================================================================

Written: output/full/report.json, output/full/dashboard.txt
```

- **Every defect found and explained** in the 316 cycles that weren't used for commissioning.
- **One cycle disagrees with its label.** Cycle 10 at 19:59 UTC, the first cycle labelled "NoNose,NoBody2", shows the three grasps of a cycle with only the nose missing, at the usual times. It is a commissioning cycle (the majority of the three decides), and the most likely explanation is that the label block starts one cycle early.
- **The generic detector's top signal** is `M_R01_UJointAngle_Degree`, one of R01's joint angles, which drifts over the run.
- **Six complete rockets** had a grasp more than 3 s late: warnings, not defects.

Through Kafka (`--transport kafka`, Kafka 4.4.0 in Docker on the same machine), the results are identical:

```
Stream:    kafka, 10,376 events/s (94.0 s)
  Process-aware (R03 grasps):  precision 1.0, recall 1.0, missing pieces exactly right 1.0 (316 cycles)
  Generic (sensor z-scores):   precision 0.532, recall 1.0 (144 complete rockets flagged; top sensor M_R01_UJointAngle_Degree)
```

The gold layer, `lake/gold/hourly_quality.csv`, has the defects per hour (the first six hours):

```
                     hour  cycles  defects  defect_rate  missing_body1  missing_body2  missing_nose
2023-12-11 13:00:00+00:00       6        0        0.000              0              0             0
2023-12-11 14:00:00+00:00      10        0        0.000              0              0             0
2023-12-11 15:00:00+00:00       8        1        0.125              0              0             1
2023-12-11 16:00:00+00:00       2        2        1.000              0              0             2
2023-12-11 17:00:00+00:00      11       11        1.000              0              0            11
2023-12-11 18:00:00+00:00      12       12        1.000              0              0            12
```

## License

MIT; see [LICENSE](LICENSE). The bundled sample and the files derived from the dataset are CC BY-SA 4.0.
