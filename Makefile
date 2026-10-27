.PHONY: smoke run data run-full kafka-up run-kafka kafka-down learn-template clean

smoke:            ## tests on the bundled sample (a few seconds)
	uv run pytest -q tests

run:              ## the pipeline on the bundled sample
	uv run python demo_pipeline.py --data sample

data:             ## download the 29-hour dataset from Kaggle (needs a Kaggle API token)
	uv run python download_data.py

run-full: data    ## the pipeline on the 29-hour dataset
	uv run python demo_pipeline.py --data full

kafka-up:         ## a local Kafka broker (Docker)
	docker compose up -d --wait kafka

run-kafka: kafka-up
	uv run python demo_pipeline.py --data sample --transport kafka

kafka-down:
	docker compose down

learn-template:   ## re-learn config/r03_template.json and config/generic_baseline.json (needs the full data)
	uv run python -m ff_pipeline.rca --learn-template

clean:
	rm -rf output .pytest_cache
