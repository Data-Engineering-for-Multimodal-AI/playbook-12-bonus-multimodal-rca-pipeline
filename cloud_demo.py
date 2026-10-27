#!/usr/bin/env python3
"""
The pipeline step by step (this script used to print a scripted walk-through; it now runs the pipeline).

    python cloud_demo.py            # all six steps
    python cloud_demo.py --step 4   # one step's output (the pipeline runs once either way)
"""
import sys

from ff_pipeline.pipeline import main

if __name__ == "__main__":
    sys.exit(main())
