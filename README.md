# Playbook 12 bonus: Find defective products on a robot assembly line
**How to run streaming root cause analysis on factory data.** Bonus for the [Chapter 12 playbook](https://github.com/Data-Engineering-for-Multimodal-AI/playbook-12-multimodal-streamkit-ops-pipeline) of [*Data Engineering for Multimodal AI*](https://oreil.ly/data-engineering-multimodal) (O'Reilly, 2026). Not printed in the book.

On an assembly line, a defective product shows up in robot and conveyor signals long before anyone inspects it. In this repo, we explore this with real data from the Future Factories testbed at the University of South Carolina. Follow along to stream 29 hours of signals through the Chapter 12 pipeline and find which assemblies went wrong, and why.

**The problem.** Machine signals arrive many times a second, and a fault is a small change hidden in them.

**What you'll work with.** You'll stream the signals through the Chapter 12 six-stage pipeline and check what it finds against the faults the dataset labels.

> The original steps are under [Run it the way the book does](#run-it-the-way-the-book-does) and still work as shown. Want the repository as it was when the book was published? Use the `first-edition` tag. This branch is the 2026 maintenance update; see [Differences from the first edition](#differences-from-the-first-edition).

## Run it

You need [uv](https://docs.astral.sh/uv/) and `make`, and Docker for the optional step 4. Or open the repo in a GitHub Codespace, where all of them are ready. On Windows without `make`, use a Codespace or the original steps below.

**1. Get the code.** The clone is about 3 MB, with the 12-cycle sample included.

```bash
git clone https://github.com/Data-Engineering-for-Multimodal-AI/playbook-12-bonus-multimodal-rca-pipeline.git
cd playbook-12-bonus-multimodal-rca-pipeline
```

**2. Run the pipeline on the sample.** Installs the locked dependencies (`uv sync`) and runs all six stages on the bundled 12 cycles, in memory (`python demo_pipeline.py --data sample`). No Docker needed. On a fresh clone this took 25 s on a Mac, most of it installing the dependencies; later runs take about 6 s.

```bash
make run
```

The last step prints a dashboard. Among its lines you should see:

```
Data:      data/sample/ff_sample.parquet: 36,166 events, 18.4 h, 12 complete cycles
Edge:      59.6 MB of raw events -> 0.025 MB forwarded (99.96% less); 9 defect alerts, 1 timing warnings
  Process-aware (R03 grasps):  precision 1.0, recall 1.0, missing pieces exactly right 1.0 (12 cycles)
  Generic (sensor z-scores):   precision 0.75, recall 1.0 (3 complete rockets flagged; top sensor M_R01_UJointAngle_Degree)
  cycle 13 (NoNose,NoBody2): body2, nose missing from the tray: R03 found nothing to grip at about 79 s into the cycle
```

The process-aware check names the missing pieces in every cycle. The generic detector also flags the three complete rockets. The full report is in `output/sample/report.json`, and the per-hour defect counts in `output/sample/lake/gold/hourly_quality.csv`.

**3. Look at one cycle yourself.** Prints R03's gripper load over the part of the cycle where it grips, then which grasp fills which slot of the template (`python scripts/cycle.py 14`). Leave out `CYCLE=` to list the 12 cycles and their labels.

```bash
make cycle CYCLE=14
```

You should see:

```
R03 gripper load, one character per 2 s from 50 s to 130 s ('@' = 9,192):
  50 s |           %***#             #%%%       | 130 s

Template slot  piece   expected  found     (cycle shifted +6.8 s)
  0            body1     72.6 s    72.6 s
  1            body2     85.6 s  nothing
  2            nose      98.6 s  nothing
  3            base     109.6 s   109.6 s
```

Cycle 14 ran 6.8 s late, and R03 gripped twice instead of four times: body2 and the nose cone weren't on the tray. Try `CYCLE=194`, a complete rocket whose last grasp came 12 s late.

**4. Stream through Kafka (optional, Docker).** Starts one Kafka broker (`docker compose up -d --wait kafka`), then runs the same pipeline with every sample published to a Kafka topic and consumed from it (`python demo_pipeline.py --data sample --transport kafka`).

```bash
make run-kafka
```

You should see `Stream:    kafka`, and the same scores as step 2. On a Mac with the Kafka image already downloaded, starting the broker took 9 s and the run 8 s (7,431 events/s; in memory it ranged from 12,600 to 30,300 events/s on the same Mac). Stop the broker with `make kafka-down`.

**5. The full 29 hours (optional, Kaggle account).** Downloads the dataset from Kaggle (`python download_data.py`), then runs the pipeline on all 325 cycles (`python demo_pipeline.py --data full`). Kaggle needs a free account and an API token; see [The full 29 hours](#the-full-29-hours) below for the setup, and [Expected output](#expected-output) for what it prints.

```bash
make run-full
```

**6. Clean up.**

```bash
make kafka-down   # if you ran step 4
make clean
```

`make clean` deletes `output/` (the reports and the Parquet lake, including `output/own/` from your own data) and the test cache. The sample, a downloaded dataset in `data/ff-analog/` and the installed packages stay.

## How much time do you have?

| Time | What to do |
|---|---|
| 15 minutes | [Run it](#run-it), steps 1 to 3. On a Mac, step 2 took 25 s on a fresh clone. |
| 1 hour | The three [Try this](#try-this) experiments (under 10 s each on a Mac), step 4 through Kafka, and `make cycle` on a few cycles. |
| 1 day | Download the full 29 hours (step 5), run the experiments on it, try [Use your own data](#use-your-own-data) and work through [Before production](#before-production). |

## Try this

Each experiment changes one thing and measures the effect on the bundled sample: 12 cycles, 9 with pieces missing and 3 complete rockets. The numbers below are from a Mac on 2026-11-21. No Docker needed.

**1. How often do you need to sample the line?** The recording is 10 Hz. This keeps every 2nd, 5th, 10th ... sample, as if the line were recorded more slowly, and runs the cycle processing and the diagnosis on each version.

```bash
make try-sample-rate
```

```
   rate  samples  raw MB  R03 grasps  precision  recall  pieces exactly right
  10 Hz   36,166    59.6          30       1.00    1.00                  1.00
   5 Hz   18,083    29.8          30       1.00    1.00                  1.00
   2 Hz    7,234    11.9          30       1.00    1.00                  1.00
   1 Hz    3,617     6.0          30       1.00    1.00                  1.00
 0.5 Hz    1,809     3.0          29       0.90    1.00                  0.92
0.33 Hz    1,206     2.0          11       0.75    1.00                  0.00
 0.2 Hz      724     1.2           1       0.75    1.00                  0.00
```

At 1 Hz every grasp is still found, from a tenth of the raw data. Below that, grasps of 6 to 8 s no longer reach the 3 samples in a row the detector needs (`min_samples` in `config/pipeline.json`), and complete rockets start to look defective. Set `MIN_SAMPLES` in `experiments/sample_rate.py` and run it again.

**2. Where do you draw the line between holding a piece and an empty gripper?** R03 counts a grasp when its load cell reads above 3,000 (`load_threshold`). This runs the sample with other thresholds.

```bash
make try-load-threshold
```

```
R03 gripper load over 36,166 samples: min 1,248, median 1,371, max 10,078

threshold  R03 grasps  precision  recall  pieces exactly right
    1,380         283       0.75    1.00                  0.25
    1,400          33       1.00    0.89                  0.83
    1,500          30       1.00    1.00                  1.00
    3,000          30       1.00    1.00                  1.00  (config/pipeline.json)
    5,000          28       0.82    1.00                  0.83
    7,000          31       0.67    0.67                  0.42
    9,000           6       0.75    1.00                  0.00
```

Any threshold from 1,500 to 3,000 gives the right answer. Just above the empty reading, noise counts as grasps; above 5,000, light pieces stop counting, and at 7,000 one grasp splits in two when the load dips. Change `THRESHOLDS` in the script, or pass `full` to run it on the 29 hours.

**3. Which signals show a missing piece?** Runs the generic z-score detector on one group of signals at a time, and the process-aware diagnosis for comparison.

```bash
make try-signal-groups
```

```
signals                     count  flagged  false alarms  precision  recall  top signal
all 45 signals                 45       12             3       0.75    1.00  M_R01_UJointAngle_Degree
conveyor                        9        4             1       0.75    0.33  Q_VFD4_Temperature
cell safety and tray            4        1             0       1.00    0.11  I_SafetyDoor2_Status
R01 (carries the tray)          8       12             3       0.75    1.00  M_R01_UJointAngle_Degree
R02 (body pieces)               8       11             3       0.73    0.89  I_R02_Gripper_Pot
R03 (builds the rocket)         8       11             2       0.82    1.00  I_R03_Gripper_Load
R04                             8        1             0       1.00    0.11  M_R04_TJointAngle_Degree
process-aware (R03 grasps)      1        9             0       1.00    1.00  I_R03_Gripper_Load
```

R01 flags every cycle: its joint angle drifts, and the detector over all 45 signals inherits that. R03's signals come closest, but z-scores of cycle averages still flag two complete rockets; counting grasps on the one R03 signal flags none. Change `Z_THRESHOLD` in `experiments/signal_groups.py` to see the trade-off.

## Use your own data

Put your recordings in a folder: `.csv` or `.parquet` files, one row per sample, in the Future Factories layout.

```
my_line/
  R03_Data.csv                 _time (or ts_ms), I_R03_Gripper_Load, other R03 signals
  FFCell_CycleManagement.csv   _time (or ts_ms), Q_Cell_CycleCount, Description (optional)
  ...                          any other sensor columns
```

- The time column is `ts_ms` (milliseconds since 1970, UTC) or `_time` (ISO 8601 text, as in the Kaggle CSVs).
- Somewhere in the folder you need the cell's cycle counter `Q_Cell_CycleCount` and R03's gripper load `I_R03_Gripper_Load`.
- `Description` is the label: empty for a complete rocket, or `NoNose`, `NoBody1`, `NoBody2` joined by commas. Without it, every cycle is scored as complete.
- Files that share timestamps are joined side by side (the Kaggle export has one file per robot); files with different times are stacked.

Check it first. This reads every file and writes nothing:

```bash
make check-data DIR=my_line
```

It reports every file it can't use and why: a file type it doesn't take, an empty or unreadable file, text that isn't UTF-8, a Git LFS pointer, a missing or unparseable time column, timestamps that repeat or go back in time, or timestamps that aren't milliseconds. Then it checks the joined table: the two required columns, missing or non-numeric values in them, unknown labels, a sample rate slower than 1 Hz ([Try this 1](#try-this)) and at least one complete cycle (240 s). On a folder with five broken files:

```
5 problems in my_line; nothing was written:
  Conveyor_Signals.csv: empty file
  R01_Data.csv: a Git LFS pointer, not the data (run git lfs pull)
  R02_Data.csv: timestamps go back in time at row 3; sort the file by time
  R04_Data.csv: no time column (ts_ms or _time)
  notes.txt: not a .csv or .parquet file
```

And with good files but bad values:

```
2 problems in my_line; nothing was written:
  I_R03_Gripper_Load: 30 samples missing or not a number (first at 2023-12-11 17:00:44 UTC)
  Description: unknown labels 'NoBase'; use empty (complete) or NoNose, NoBody1, NoBody2 separated by commas
```

When everything passes, run the pipeline on it:

```bash
make load DIR=my_line
```

```
my_line: OK
  36,166 samples, 17 signals, 18.4 h, 12 complete cycles
...
Data:      output/own/ff_own.parquet: 36,166 events, 18.4 h, 12 complete cycles
```

This joins the files into `output/own/ff_own.parquet` and writes the report, dashboard and lake next to it. To look at one of your cycles: `uv run python scripts/cycle.py N --data output/own/ff_own.parquet`. The R03 template and the generic baseline were learned on the Future Factories line; a line with other timings needs its own (see `learn_template` and `GenericDetector.calibrate` in `ff_pipeline/rca.py`).

## What you end up with

A streaming pipeline that turns a factory's 10 Hz sensor stream into one diagnosed summary per assembly cycle, with a Parquet lake, a report, and a checked loader for your own recordings.

Skills you'll practice:
- Turning a stream of samples into windows (cycles) and summarizing each one as it closes
- Building a detector from knowledge of the process, and scoring it against labels
- Comparing it with a generic anomaly detector, and seeing where drift fools it
- Deciding what to send upstream from the edge, and measuring the bytes saved
- Validating input files before they reach the pipeline
- Measuring sample rate and thresholds before choosing them

## Before production

This playbook replays a recording on one machine. Before you run something like it on a live line:

1. **Validate every event where it arrives.** `CycleProcessor` takes any event; a sample with no `I_R03_Gripper_Load` simply ends a grasp. When we blanked the load in complete cycle 192 from 90 to 100 s, the diagnosis was "nose missing from the tray". Check each event's fields and range at the edge, and alert on a silent sensor separately from a defect. *Chapter 12, "Schema Validation at the Edge" (page 469).*
2. **Choose the sample rate by measuring it.** The line records at 10 Hz, but every grasp was still found at 1 Hz, from 6.0 MB of raw data instead of 59.6 MB ([Try this 1](#try-this)). Below 0.5 Hz the diagnosis fails. *Chapter 7, "Partitioning Logic: What Runs Where and Why" (page 243); Chapter 12, "The Edge-Cloud Handoff" (page 471).*
3. **Calibrate the grasp threshold per gripper.** `load_threshold` is one fixed number, 3,000, for one load cell. On the sample, anything from 1,500 to 3,000 works and 5,000 already misses grasps ([Try this 2](#try-this)). Another gripper or a recalibrated cell needs its own value, checked on known cycles. *Chapter 8, "Testing and Validation for Multimodal Models" (page 286).*
4. **Plan for drift.** The generic detector's baseline comes from the first 10 cycles and never changes. On the 29 hours it flags 144 of 161 complete rockets because one R01 joint angle drifts ([Try this 3](#try-this) shows the same on the sample). Re-learn baselines on a schedule, or use signals tied to the process. *Chapter 7, "Drift Types, Detection, and Engineering Response" (page 229).*
5. **Promote a new template in shadow mode.** `make learn-template` overwrites `config/r03_template.json`, and the next run uses it. Run a new template next to the current one, compare their diagnoses, then switch. *Chapter 12, "Shadow Mode and Safe Promotion" (page 484).*
6. **Make the Kafka path restartable.** Every run creates a new topic and a new consumer group, never commits offsets, and never deletes the topic; the broker has one partition and replication factor 1. A consumer that restarts should resume where it stopped, and old topics need a retention policy. *Chapter 12, "Pattern 2: Transport Selection" (page 457) and "Pattern 3: Back-Pressure and Fault Handling" (page 459).*
7. **Watch the pipeline while it runs.** The dashboard is written once, after the stream ends. Export events per second, cycles closed, alerts and late grasps as live metrics, and link each alert to a runbook. *Chapter 12, "Signal Granularity: Instrument Each Lane" (page 474) and "Automated Runbooks for Common Failures" (page 481).*
8. **Manage the lake's files.** Bronze is written in batches of 50,000 events, one file per hour per batch, and silver and gold are rewritten on every run. A live line needs append-only writes, compaction and a schema that can evolve. *Chapter 2, "Composable Data Lakehouse Architecture" (page 37).*
9. **Keep the data's license and lineage with it.** The sample and anything derived from it are CC BY-SA 4.0, and the template records which cycles it was learned from. Record source, license and version for every recording you load. *Chapter 11, "Lineage and Provenance Tracking" (page 416).*

## What it costs

| Where you run it | Cost |
|---|---|
| Your machine | $0. Everything runs locally: no API keys, no paid services. A Kaggle account (free) only for the full 29 hours. The clone is about 3 MB and the installed packages about 240 MB. |
| A GitHub Codespace (2 cores, 8 GB, the default size; this repo doesn't ask for more) | Estimated $0.18 per hour at GitHub's list price (November 2026). A free personal account includes 120 core-hours a month, about 60 hours on this machine. |

## How it works

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

## Run it the way the book does

The book doesn't print this repository; these are the original steps, and they still work as shown. You need Python 3.12 or newer.

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
