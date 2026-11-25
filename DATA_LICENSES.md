# Data licenses

This file lists the datasets and models this playbook uses, where they come from, and their licenses. The code in this repository is MIT-licensed. That license does not cover third-party data or models; each one keeps its own license, listed below.

## Datasets

| Dataset | Source | License | In this repo |
|---|---|---|---|
| Future Factories analog dataset, 12-cycle sample (36,166 rows at 10 Hz, three cycles of each label) | Adapted from the dataset below by `python download_data.py --make-sample` | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/), as the source requires | `data/sample/ff_sample.parquet` (1.4 MB). See `data/sample/README.md` |
| Future Factories analog dataset (about 30 hours of a rocket assembly line, four robots and a conveyor at 10 Hz) | [Kaggle: ramyharik/ff-2023-12-12-analog-dataset](https://www.kaggle.com/datasets/ramyharik/ff-2023-12-12-analog-dataset), Future Factories lab, University of South Carolina | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) | No. Downloaded by `make data` (`python download_data.py`) into `data/ff-analog/` (about 30 MB as Parquet). Needs a Kaggle API token |
| Cycle template and baseline | Learned from the dataset (first 10 cycles, plus 9 labelled cycles for the piece names) | [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (derived from the dataset) | `config/r03_template.json` and `config/generic_baseline.json` (4 KB) |

No pretrained models are used.

## Notes

- The Future Factories data is CC BY-SA 4.0 (attribution, share-alike). That license, not this repository's MIT license, governs use of the bundled sample, the derived config files and the downloaded data.

## Citations

- Future Factories: R. Harik, F. El Kalach, J. Samaha, D. Clark, D. Sander, P. Samaha, L. Burns, I. Yousif, V. Gadow, T. Tarekegne and N. Saha, "Analog and Multi-modal Manufacturing Datasets Acquired on the Future Factories Platform", arXiv:2401.15544, 2024.
