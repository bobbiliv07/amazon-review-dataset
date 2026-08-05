# Amazon Review Dataset — EDA with PySpark

This repository demonstrates an end-to-end exploratory data analysis (EDA) workflow for an Amazon product reviews dataset using PySpark. The content and structure are based on the included presentation (big_data.pptx) and the accompanying `pyspark_eda.py` script.

## Project overview

Purpose
- Show how to load, preprocess, and explore a large Amazon reviews dataset using PySpark.
- Provide common EDA queries and patterns (distributional statistics, top entities, temporal trends, text-length analysis) that scale to big datasets.

Key concepts (from presentation)
- What is Big Data and why Spark: handling large volumes, distributed compute, and in-memory processing.
- PySpark basics: SparkSession, DataFrame API, common transformations and aggregations.
- EDA at scale: sampling, schema inference vs explicit schema, partitioning and caching for performance.
- Practical EDA queries: counts, distinct counts, group-bys, aggregations, review-length statistics, and time-based trends.

## Repository contents

- `pyspark_eda.py` — PySpark script that loads a CSV of Amazon reviews, preprocesses fields, and runs a set of EDA queries and example aggregates.
- `big_data.pptx` — Presentation that motivated this project and gives background on big data, Spark, and the analysis approach.
- `README.md` — This file.
- `data/` (not committed) — Expected location for the input CSV(s).

## Dataset

Expected format
- CSV with header. Typical columns used by the script (if present): `reviewerID`, `asin`, `overall`, `reviewText`, `summary`, `unixReviewTime`.

Example source
- The dataset in this repository is the Amazon reviews format (public Amazon review datasets are available from several academic mirrors). This repo does not include a hosted dataset — place your CSV at `data/amazon_reviews.csv` or update INPUT_PATH in the script.

## How the code works

Main steps performed by `pyspark_eda.py`:
1. Create a SparkSession.
2. Load the CSV with header and (by default) inferred schema.
3. Print basic overview (schema, row/column counts).
4. Preprocess:
   - Keep expected columns if present.
   - Drop rows missing key fields (reviewerID, asin, overall).
   - Convert `unixReviewTime` to a timestamp and extract year/month.
5. Run 10 example EDA queries:
   - Total reviews
   - Number of distinct reviewers
   - Number of distinct products
   - Rating distribution
   - Average rating overall
   - Top 10 most reviewed products
   - Top 10 most active reviewers
   - Average rating per product (top 10 by review count)
   - Review text length statistics (avg/min/max)
   - Reviews trend by year and month

## Usage

Requirements
- Python 3.8+
- Apache Spark / PySpark (tested with Spark 3.x)

Install PySpark (if needed):
pip install pyspark

Run locally
1. Place your dataset at `data/amazon_reviews.csv` (or change `INPUT_PATH` at top of `pyspark_eda.py`).
2. Run:
python pyspark_eda.py

Or submit to a Spark cluster:
spark-submit pyspark_eda.py

Command-line improvements (recommended)
- Passing input/output paths and other params via CLI or environment variables makes the script more flexible. Consider adding `argparse` to accept `--input`, `--sample`, `--limit`, `--output-path`, etc.

## Performance tips (from presentation)

- For very large datasets, avoid `inferSchema` and provide an explicit schema to speed up read.
- Use repartitioning or coalesce when writing results to control parallelism.
- Cache intermediate DataFrames if they are reused (e.g., after expensive transformations).
- Limit `.count()` calls on very large datasets or run them selectively — they trigger full jobs.

## Example outputs and artifacts

- The script prints DataFrame schema, sample rows, and the results of the example queries to stdout. For production analyses, write summary tables to Parquet/CSV and create visualizations in a downstream step (Jupyter/PowerBI/Matplotlib/Seaborn) after aggregating.

## Next steps and enhancements

- Add CLI support (`argparse`) and improve runtime configurability.
- Write query outputs to `output/` in Parquet/CSV for downstream visualization.
- Add unit tests or small integration tests using a tiny sample CSV.
- Add logging and progress reporting for long-running jobs.
- Convert the slides to a short markdown-based notebook (slides -&gt; notebook) for reproducible demonstrations.

## License

MIT License

## Acknowledgements

This repo was created to accompany the `big_data.pptx` presentation and demonstrates core patterns for EDA at scale using PySpark.
