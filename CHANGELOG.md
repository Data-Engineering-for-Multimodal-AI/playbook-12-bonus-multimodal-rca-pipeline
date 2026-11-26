# Changelog

## v1.1.0 (2026-11-26)

Nothing the book shows has changed.

### Added
- `make cycle CYCLE=N` (`scripts/cycle.py`): one cycle's R03 gripper load, its grasps and the
  template slot each one fills.
- `make check-data DIR=...` and `make load DIR=...` (`scripts/own_data.py`): check a folder of your
  own recordings (.csv or .parquet, the Future Factories layout) and run the pipeline on it
  (`output/own/`). Tests in `tests/test_own_data.py`; `make smoke` still runs only the 5 smoke tests.
- Three measured experiments on the bundled sample: `make try-sample-rate`, `make try-load-threshold`,
  `make try-signal-groups` (`experiments/`).
- README: Run it, time tiers, Try this, Use your own data, What you end up with, Before production,
  What it costs.
- CI: the Run it steps with checks on their output, the three experiments, the own-data tests,
  `make run-kafka` in the Kafka job, and the time to first output in the devcontainer job.
- `make setup`. `make run` now depends on it and runs the same command as before (the pipeline on
  the bundled sample, in memory). `make clean`, `make smoke` and the Kafka targets are unchanged,
  except that every `uv run` uses `--frozen`.
- `.devcontainer/` so the repo opens in GitHub Codespaces with dependencies installed, with
  Docker-in-Docker so `make kafka-up` and `make run-kafka` work there.
- CI: a weekly scheduled run, a `make run` step, `python demo_pipeline.py --data sample` in the
  pip install step, and a job that builds the devcontainer and runs `make run` inside it.
- `DATA_SOURCES.md`: the datasets and models this playbook uses, with their licenses and citations.
- `make run` writes `.playbook-done` when it passes (ignored by git). `make card` in the
  organization's `.github` repository reads these files and draws a completion card.

### Changed
- README: "What you'll build" is now "How it works"; "Quick start" is now "Run it the way the book
  does", with the same content.
- README opening: a title that says what you'll do, three sentences on why, the example and what you'll do, then the data engineering problem and what you'll work with.
- README: a header image (light and dark), badges for CI, Python, license and Codespaces, a diagram at
  the top of "How it works", and a footer that links the book, all playbooks, and the previous and
  next playbook.
- README: a Troubleshooting section with three common problems and their fixes.

### Fixed
- The Kafka producer runs with `enable.idempotence`, so a retried batch can no longer land after later
  events. One CI run split a cycle in two and scored 11 cycles instead of 12; that reordering is the
  likely cause.
  The producer also retries the end marker when its queue is full, and fails if Kafka does not confirm
  every event within 60 s.

## 2026.10 (2026-10-27)

The first edition is tagged `first-edition`. It didn't run: four of its five scripts didn't compile,
the fifth printed a scripted walk-through, and `docker-compose.yml` wasn't valid. This release builds
the pipeline it described, on the real dataset, and measures it.

### Added
- `ff_pipeline/`: a streaming pipeline on the Future Factories analog dataset: ingest, stream (in
  memory or Kafka), cycle processing, edge diagnosis, a bronze/silver/gold Parquet lake, and a report.
- Root cause analysis per assembly cycle: which pieces were missing, from R03's grasps against a
  template learned from the first cycles and commissioned from 9 labelled ones. Scored against the
  dataset's labels, next to a generic z-score detector.
- `download_data.py`: downloads the dataset from Kaggle and checks its files' SHA-256.
- A 12-cycle sample (`data/sample/`, CC BY-SA 4.0) so everything runs without a Kaggle account.
- Tests, a Makefile, CI on Python 3.12 and 3.13 (pip and uv), and a CI job through a real Kafka broker.

### Changed
- `demo_pipeline.py` and `cloud_demo.py` (`--step N`) run the pipeline.
- `docker-compose.yml`: one Kafka 4.4 broker in KRaft mode.
- Dependencies: the 5 packages the code uses, pinned (`uv.lock`, `requirements.txt`); Python 3.12+.
- README: measured results only; the first edition's performance figures were not measured.
- ACKNOWLEDGMENTS: the dataset's authors and the funding their paper states.

### Removed
- `edge/edge_inference.py`, `streaming/flink_processor.py`, `streaming/gcp_kafka_producer.py` (they
  didn't compile) and `TUTORIAL.md` (it described services and steps that didn't exist).
