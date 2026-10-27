# Contributing

This repository is bonus content for *Data Engineering for Multimodal AI* (O'Reilly, 2026). Issues and
pull requests that fix bugs or keep it running on current Python and libraries are welcome.

1. `uv sync` (or `pip install -r requirements.txt` in a virtual environment).
2. Make the change; keep `make smoke` passing (it runs on the bundled sample in seconds).
3. If the change affects results, run `python demo_pipeline.py --data full` on the downloaded data and
   update the README's expected output with what you measured.
4. Dependencies: edit `pyproject.toml`, run `uv lock`, then regenerate `requirements.txt` with the
   command in its first line.
