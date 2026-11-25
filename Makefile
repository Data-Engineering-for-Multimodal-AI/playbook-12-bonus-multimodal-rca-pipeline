.PHONY: setup smoke run cycle check-data load try-sample-rate try-load-threshold try-signal-groups data run-full kafka-up run-kafka kafka-down learn-template clean

# A bare `make` runs the smoke test, as it did before the uniform targets were added.
.DEFAULT_GOAL := smoke

# Uniform targets for The Playbooks. The book doesn't print this repository. The commands in the
# README (pip install -r requirements.txt, python demo_pipeline.py --data sample) still work as shown.

setup:            ## install the locked dependencies into .venv (the sample is in the repository)
	uv sync --frozen

smoke:            ## tests on the bundled sample (a few seconds); the own-data tests: pytest tests/test_own_data.py
	uv run --frozen pytest -q tests/test_smoke.py

run: setup        ## the pipeline on the bundled sample (in memory, no Kafka)
	uv run --frozen python demo_pipeline.py --data sample
	@echo "playbook-12-bonus-multimodal-rca-pipeline" > .playbook-done

cycle: setup      ## one cycle's R03 grasps against the template: make cycle CYCLE=14 (no CYCLE: list them)
	uv run --frozen python scripts/cycle.py $(CYCLE)

# Your own recordings: .csv or .parquet files in DIR (see README, "Use your own data").
# check-data checks every file and writes nothing; load joins them and runs the pipeline (output/own/).
#   make check-data DIR=my_line
#   make load DIR=my_line
check-data: setup
	@test -n "$(DIR)" || { echo "Usage: make check-data DIR=path/to/folder"; exit 2; }
	uv run --frozen python scripts/own_data.py --dir "$(DIR)" --check-only

load: setup
	@test -n "$(DIR)" || { echo "Usage: make load DIR=path/to/folder"; exit 2; }
	uv run --frozen python scripts/own_data.py --dir "$(DIR)"

# Try this: three measured experiments on the bundled sample (see README, "Try this"). No Docker.
try-sample-rate: setup
	uv run --frozen python experiments/sample_rate.py

try-load-threshold: setup
	uv run --frozen python experiments/load_threshold.py

try-signal-groups: setup
	uv run --frozen python experiments/signal_groups.py

data:             ## download the 29-hour dataset from Kaggle (needs a Kaggle API token)
	uv run --frozen python download_data.py

run-full: data    ## the pipeline on the 29-hour dataset
	uv run --frozen python demo_pipeline.py --data full

kafka-up:         ## a local Kafka broker (Docker)
	docker compose up -d --wait kafka

run-kafka: kafka-up
	uv run --frozen python demo_pipeline.py --data sample --transport kafka

kafka-down:
	docker compose down

learn-template:   ## re-learn config/r03_template.json and config/generic_baseline.json (needs the full data)
	uv run --frozen python -m ff_pipeline.rca --learn-template

clean:            ## remove output/ (including output/own/) and test caches (the sample in data/sample/ is kept)
	rm -rf output .pytest_cache
