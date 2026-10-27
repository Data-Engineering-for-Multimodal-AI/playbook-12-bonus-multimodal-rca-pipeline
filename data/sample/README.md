# Sample: 12 cycles of the Future Factories analog dataset

`ff_sample.parquet`: 36,166 rows at 10 Hz, three consecutive cycles of each label (a complete rocket,
NoNose, NoNose+NoBody2, NoNose+NoBody2+NoBody1), with the seven CSV files of the original joined on
their shared time column and `_time` converted to `ts_ms` (milliseconds since 1970, UTC). Made by
`python download_data.py --make-sample`.

**Source:** R. Harik, F. El Kalach, J. Samaha, D. Clark, D. Sander, P. Samaha, L. Burns, I. Yousif,
V. Gadow, T. Tarekegne and N. Saha, "Analog and Multi-modal Manufacturing Datasets Acquired on the Future
Factories Platform", arXiv:2401.15544 (2024).
[Kaggle: ramyharik/ff-2023-12-12-analog-dataset](https://www.kaggle.com/datasets/ramyharik/ff-2023-12-12-analog-dataset).

**License:** this sample is an adaptation of that dataset and is shared under the same license,
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/), not the repository's MIT license.
