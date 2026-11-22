.PHONY: setup smoke run data run-full kafka-up run-kafka kafka-down learn-template clean

# A bare `make` runs the smoke test, as it did before the uniform targets were added.
.DEFAULT_GOAL := smoke

# Uniform targets for The Playbooks. The book doesn't print this repository. The commands in the
# README (pip install -r requirements.txt, python demo_pipeline.py --data sample) still work as shown.

setup:            ## install the locked dependencies into .venv (the sample is in the repository)
	uv sync --frozen

smoke:            ## tests on the bundled sample (a few seconds)
	uv run --frozen pytest -q tests

run: setup        ## the pipeline on the bundled sample (in memory, no Kafka)
	uv run --frozen python demo_pipeline.py --data sample

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

clean:            ## remove output/ and test caches (the sample in data/sample/ is kept)
	rm -rf output .pytest_cache
