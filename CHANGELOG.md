# Changelog

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
