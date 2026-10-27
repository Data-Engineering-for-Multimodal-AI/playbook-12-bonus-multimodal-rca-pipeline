#!/usr/bin/env python3
"""Run the whole pipeline: python demo_pipeline.py [--data sample|full] [--transport memory|kafka]"""
import sys

from ff_pipeline.pipeline import main

if __name__ == "__main__":
    sys.exit(main())
