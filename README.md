# Amazon Review Dataset Analysis Using Big Data Architecture

**Author:** BOBBILI VAMSHI  
**Project Focus:** Distributed data engineering, PySpark analytics, HDFS, and the Medallion Architecture

## Overview

This project analyzes the **Amazon Reviews 1996–2023 dataset** using a scalable big-data workflow. It combines customer reviews with product metadata to identify trends in customer sentiment, product quality, review activity, purchasing signals, and category performance.

The solution is designed to demonstrate how large, semi-structured datasets can be ingested, stored, transformed, and analyzed using **Hadoop HDFS** and **Apache PySpark**. The project also follows the **Medallion Architecture**, separating data into Bronze, Silver, and Gold layers to improve data quality, traceability, and analytical usability.

> **Important:** The dataset contains review and product information. Review counts, ratings, helpful votes, and product metadata should be interpreted as analytical indicators rather than direct sales or revenue measurements.

## Project Objectives

- Ingest large-scale Amazon review and product metadata files.
- Store raw data in HDFS using a fault-tolerant distributed storage model.
- Process semi-structured JSON/JSONL data with PySpark.
- Clean, validate, standardize, and enrich review and product records.
- Join reviews with item metadata using `parent_asin`.
- Produce curated analytical datasets for exploratory analysis and reporting.
- Identify highly reviewed, highly rated, and influential products and categories.
- Analyze rating distributions, verified purchases, helpful votes, and review activity over time.
- Demonstrate an extensible data pipeline based on the Medallion Architecture.

## Architecture

The project follows this high-level pipeline:

```text
Amazon Reviews and Metadata
            |
            v
      Data Ingestion
            |
            v
     HDFS Raw Storage
            |
            v
   Bronze Layer - Raw Data
            |
            v
  Silver Layer - Cleaned Data
            |
            v
   Gold Layer - Analytics
            |
            v
 Insights, Reports, and Visualizations
```

### Technologies Used

| Technology | Purpose |
| :--- | :--- |
| **Python** | Scripting, data preparation, and analysis orchestration |
| **Apache Spark / PySpark** | Distributed processing, transformation, joins, and aggregation |
| **Hadoop HDFS** | Distributed and fault-tolerant storage for large datasets |
| **Spark SQL / DataFrames** | Structured querying and analytical transformations |
| **JSON / JSONL** | Source data format for reviews and product metadata |
| **Jupyter Notebook / IDE** | Development, experimentation, and analysis |

## Medallion Architecture

The data pipeline is organized into three logical layers. Each layer has a specific responsibility and provides a clear path from raw data to business-ready insights.

### Bronze Layer - Raw Ingestion

The Bronze layer stores the source data with minimal changes. Its purpose is to preserve the original records and provide a reliable source for reprocessing.

Typical Bronze activities include:

- Downloading review and metadata files.
- Copying source files into HDFS.
- Preserving the original JSON/JSONL structure.
- Recording ingestion locations and source versions.
- Applying only basic parsing or schema inference where required.

Example HDFS locations:

```text
/user/hadoop/raw_data/reviews/
/user/hadoop/raw_data/metadata/
```

### Silver Layer - Cleaned and Integrated Data

The Silver layer contains validated, standardized, and enriched records prepared for analysis.

Typical Silver transformations include:

- Selecting relevant columns.
- Casting fields to appropriate data types.
- Converting Unix timestamps into readable dates.
- Handling null, malformed, or duplicate records.
- Normalizing rating, price, vote, and Boolean fields.
- Exploding or flattening nested arrays such as `features`, `categories`, and `images` when required.
- Joining reviews with product metadata using `parent_asin`.
- Adding derived fields such as review year, review month, rating group, and purchase-verification indicators.

Example HDFS locations:

```text
/user/hadoop/silver/reviews_cleaned/
/user/hadoop/silver/product_metadata_cleaned/
/user/hadoop/silver/reviews_enriched/
```

### Gold Layer - Curated Analytics

The Gold layer contains aggregated, business-friendly datasets that can be used for dashboards, reports, visualizations, and downstream machine-learning tasks.

Possible Gold datasets include:

- Product-level review summaries.
- Category-level rating and review metrics.
- Yearly and monthly review trends.
- Verified versus non-verified purchase comparisons.
- Helpful-vote and review-engagement summaries.
- Top-rated products with sufficient review volume.
- Products with the highest number of reviews.
- Average rating and rating-count comparisons.
- Price, category, and review-performance summaries.

Example HDFS locations:

```text
/user/hadoop/gold/product_summary/
/user/hadoop/gold/category_summary/
/user/hadoop/gold/time_series_metrics/
```

## Data Sources

### Primary Download Source

- **Dataset:** Amazon Reviews and Metadata Dataset
- **Source:** [McAuley Lab - UCSD Amazon Reviews Dataset](http://jmcauley.ucsd.edu/data/amazon/)
- **Original compiler:** Julian McAuley and collaborators
- **Version used:** Amazon Reviews Data and Metadata, 2023
- **Coverage:** Review records and product metadata spanning approximately 1996–2023

The source data is semi-structured and may contain nested arrays, dictionaries, missing values, and inconsistent fields. The pipeline prepares this data for distributed analysis using Spark DataFrames.

## Data Schema

### User Review Fields

| Field | Type | Description |
| :--- | :--- | :--- |
| `rating` | `float` | Product rating from 1.0 to 5.0. |
| `title` | `string` | Title of the review. |
| `text` | `string` | Full review text. |
| `images` | `array` | User-submitted image information. |
| `asin` | `string` | Identifier of the reviewed product or variation. |
| `parent_asin` | `string` | Parent product identifier used for metadata joins. |
| `user_id` | `string` | Reviewer identifier. |
| `timestamp` | `long` | Review timestamp in Unix epoch format. |
| `verified_purchase` | `boolean` | Indicates whether the purchase was verified. |
| `helpful_vote` | `integer` | Number of helpful votes received by the review. |

### Product Metadata Fields

| Field | Type | Description |
| :--- | :--- | :--- |
| `main_category` | `string` | Primary product category. |
| `title` | `string` | Product title. |
| `average_rating` | `float` | Average rating displayed for the product. |
| `rating_number` | `integer` | Number of ratings associated with the product. |
| `features` | `array` | Product bullet-point features. |
| `description` | `array` | Product description content. |
| `price` | `float` | Product price in USD when available. |
| `images` | `array` | Product image information. |
| `videos` | `array` | Product video titles and URLs. |
| `store` | `string` | Store or seller name. |
| `categories` | `array` | Product category hierarchy. |
| `details` | `map/struct` | Additional product attributes such as brand, material, or size. |
| `parent_asin` | `string` | Parent product identifier. |
| `bought_together` | `array` | Recommended product bundles when available. |

## Key Analytics and Insights

The pipeline can be used to generate insights such as:

1. Which product categories receive the highest volume of reviews?
2. Which products have the highest average ratings after applying a minimum-review threshold?
3. How do ratings and review activity change from year to year?
4. Do verified purchases have different rating or helpful-vote patterns?
5. Which products receive the most helpful votes?
6. Which stores or categories have the strongest review performance?
7. How do product price and review activity relate to one another?
8. Which products have substantial review activity and long-term visibility?

To avoid misleading conclusions, rankings should use appropriate thresholds. For example, a product with one five-star review should not automatically be considered better than a product with thousands of consistently positive reviews.

## HDFS Storage and Ingestion

HDFS is used as the distributed storage layer for large input and output files. It provides scalability, data locality, and fault tolerance across a Hadoop cluster.

### HDFS Benefits

- **Distributed storage:** Files are split into blocks and distributed across cluster nodes.
- **Fault tolerance:** Replication helps protect data from node failures.
- **Scalability:** Storage capacity can grow by adding nodes.
- **Data locality:** Processing can occur close to where the data is stored.
- **Large-file support:** HDFS is optimized for high-throughput batch workloads.

### Example Ingestion Commands

The exact paths and user names should be adjusted for the local Hadoop environment.

```bash
# Switch to the Hadoop user and start Hadoop services
sudo su - hadoop
start-all.sh

# Create HDFS directories
hdfs dfs -mkdir -p /user/hadoop/raw_data/reviews
hdfs dfs -mkdir -p /user/hadoop/raw_data/metadata

# Upload source files to HDFS
hdfs dfs -put /home/hadoop_user/amazon_reviews.json /user/hadoop/raw_data/reviews/
hdfs dfs -put /home/hadoop_user/amazon_metadata.json /user/hadoop/raw_data/metadata/

# List files and verify the upload
hdfs dfs -ls -R /user/hadoop/raw_data

# Optional: upload with explicit replication and block-size settings
hdfs dfs -D dfs.replication=1 \
  -D dfs.block.size=1073741824 \
  -put /home/hadoop_user/amazon_reviews.json /user/hadoop/raw_data/reviews/
```

> In a production or multi-node cluster, use the replication factor configured by the Hadoop administrator. A replication factor of `1` is generally appropriate only for local development or a single-node demonstration environment.

## PySpark Processing Workflow

A typical PySpark job follows these stages:

1. Create a `SparkSession` configured for local or cluster execution.
2. Read review and metadata files from HDFS.
3. Apply explicit or inferred schemas.
4. Clean null values, malformed records, duplicates, and invalid ratings.
5. Convert timestamps into date and time dimensions.
6. Flatten nested fields when required for analysis.
7. Join review records with product metadata on `parent_asin`.
8. Create Silver-layer datasets in a columnar format such as Parquet.
9. Aggregate metrics into Gold-layer datasets.
10. Write results to HDFS for reporting and visualization.

Example PySpark pattern:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year

spark = (
    SparkSession.builder
    .appName("AmazonReviewAnalysis")
    .getOrCreate()
)

reviews = spark.read.json("hdfs:///user/hadoop/raw_data/reviews/")
metadata = spark.read.json("hdfs:///user/hadoop/raw_data/metadata/")

silver_reviews = (
    reviews
    .withColumn("review_date", from_unixtime(col("timestamp")).cast("date"))
    .withColumn("review_year", year(col("review_date")))
    .filter(col("rating").between(1.0, 5.0))
    .dropDuplicates(["user_id", "parent_asin", "timestamp", "text"])
)

silver_reviews.write.mode("overwrite").parquet(
    "hdfs:///user/hadoop/silver/reviews_cleaned/"
)

spark.stop()
```

## Data Quality Practices

The pipeline should validate data before creating analytical outputs:

- Confirm that ratings are between 1.0 and 5.0.
- Check that `parent_asin` is present when a metadata join is required.
- Standardize missing text, category, price, and store values.
- Remove exact duplicate review records where appropriate.
- Validate timestamp ranges and derive calendar fields consistently.
- Apply minimum review-count thresholds to product rankings.
- Record unmatched review and metadata records after joins.
- Preserve raw Bronze data so transformations can be reproduced.

## Output Format and Performance Considerations

For repeated analytics, curated Silver and Gold datasets can be stored in **Parquet** rather than JSON. Parquet provides schema information, columnar storage, compression, and more efficient column-based reads.

Recommended optimization techniques include:

- Partitioning time-series outputs by `review_year` or `review_date`.
- Selecting only the columns needed for each transformation.
- Persisting reused DataFrames when beneficial.
- Broadcasting small lookup datasets where appropriate.
- Avoiding unnecessary `collect()` operations on large DataFrames.
- Repartitioning carefully before large joins and aggregations.
- Using Spark UI to inspect stages, shuffles, and execution plans.

## Repository Contents

The repository contains project implementation materials, including the data-processing code and presentation/documentation artifacts. The code and presentation demonstrate the use of PySpark, HDFS, data transformation, and analytical reporting.

File names may vary as the project evolves. Refer to the repository tree for the latest notebooks, scripts, presentations, and supporting files.

## Limitations

- The dataset is a snapshot and may not represent current Amazon inventory, prices, or review activity.
- Review activity is not the same as product sales or revenue.
- Product prices may be missing or reflect only the time of metadata collection.
- A product's average rating can be affected by review volume, product variations, and sampling.
- Large files require sufficient local disk space, memory, and Hadoop/Spark configuration.
- Some nested fields may require additional normalization before analysis.

## Future Enhancements

- Add automated Bronze-to-Silver-to-Gold orchestration with Airflow or another scheduler.
- Add data-quality checks using Great Expectations or custom validation jobs.
- Build dashboards using Power BI, Tableau, or Apache Superset.
- Add sentiment analysis and topic modeling for review text.
- Use Delta Lake or Apache Iceberg for ACID transactions and schema evolution.
- Add incremental ingestion and partition-aware processing.
- Implement product recommendation or rating-prediction models.
- Add automated tests for schemas, transformations, and output quality.

## Conclusion

This project demonstrates an end-to-end approach for analyzing large-scale customer feedback with distributed technologies. By combining HDFS for storage, PySpark for processing, and the Medallion Architecture for data organization, the solution creates a reliable path from raw Amazon data to clean, reusable, and analytically valuable datasets.
