# Amazon Review Dataset - EDA with PySpark

This repository contains scripts for exploratory data analysis (EDA) of an Amazon product reviews dataset using PySpark.

## Contents
- `pyspark_eda.py` — ETL + EDA example using PySpark. Loads a CSV of Amazon reviews, performs basic preprocessing and runs a set of common EDA queries (counts, rating distribution, top products/reviewers, review length stats, trends by year/month).

## Requirements
- Apache Spark with PySpark (tested with Spark 3.x)
- Python 3.8+

Install PySpark (if needed):

pip install pyspark

## Usage
1. Place your dataset at `data/amazon_reviews.csv` (or update `INPUT_PATH` in `pyspark_eda.py`). The script expects a CSV with a header and (optionally) a `unixReviewTime` column.

2. Run locally:

python pyspark_eda.py

Or submit to a Spark cluster:

spark-submit pyspark_eda.py

## Notes
- The script attempts to infer schema from the CSV. For large datasets, consider using `spark.read.csv(...).schema(...)` with an explicit schema to speed up loading.
- Expected columns (if present) the script uses: `reviewerID`, `asin`, `overall`, `reviewText`, `summary`, `unixReviewTime`.

## License
MIT
