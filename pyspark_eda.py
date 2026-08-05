from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, min as spark_min, max as spark_max,
    length, to_timestamp, from_unixtime, year, month, desc
)

# =========================
# Configuration
# =========================
# Local example: INPUT_PATH = "data/amazon_reviews.csv"
# HDFS example: INPUT_PATH = "hdfs:///user/hadoop/amazon_reviews.csv"
INPUT_PATH = "data/amazon_reviews.csv"


def create_spark_session(app_name: str = "AmazonReviewsEDA") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def load_data(spark: SparkSession, path: str):
    # Assumes CSV with header. Change format/options if your file differs (JSON/Parquet/etc.)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    return df


def basic_overview(df):
    print("\n=== BASIC OVERVIEW ===")
    df.printSchema()
    total_rows = df.count()
    print(f"Total rows: {total_rows}")
    print(f"Total columns: {len(df.columns)}")


def preprocess(df):
    # Keep only expected columns if they exist
    expected_cols = [
        "reviewerID", "asin", "overall", "reviewText", "summary", "unixReviewTime"
    ]
    available_cols = [c for c in expected_cols if c in df.columns]
    df = df.select(*available_cols)

    # Drop rows missing key fields (if those fields exist)
    required = [c for c in ["reviewerID", "asin", "overall"] if c in df.columns]
    if required:
        df = df.dropna(subset=required)

    # Convert unix timestamp to readable timestamp if present
    if "unixReviewTime" in df.columns:
        df = df.withColumn("review_timestamp", to_timestamp(from_unixtime(col("unixReviewTime"))))
        df = df.withColumn("review_year", year(col("review_timestamp")))
        df = df.withColumn("review_month", month(col("review_timestamp")))

    return df


def run_queries(df):
    print("\n=== 10 EDA QUERIES ===")

    # 1) Total reviews
    print("\nQ1. Total number of reviews")
    print(df.count())

    # 2) Distinct users
    if "reviewerID" in df.columns:
        print("\nQ2. Number of distinct reviewers")
        print(df.select("reviewerID").distinct().count())

    # 3) Distinct products
    if "asin" in df.columns:
        print("\nQ3. Number of distinct products")
        print(df.select("asin").distinct().count())

    # 4) Rating distribution
    if "overall" in df.columns:
        print("\nQ4. Rating distribution")
        df.groupBy("overall").agg(count("*").alias("review_count")).orderBy("overall").show(20, truncate=False)

    # 5) Average rating overall
    if "overall" in df.columns:
        print("\nQ5. Average rating overall")
        df.select(avg("overall").alias("avg_rating")).show(truncate=False)

    # 6) Top 10 most reviewed products
    if "asin" in df.columns:
        print("\nQ6. Top 10 most reviewed products")
        (
            df.groupBy("asin")
            .agg(count("*").alias("review_count"))
            .orderBy(desc("review_count"))
            .show(10, truncate=False)
        )

    # 7) Top 10 most active reviewers
    if "reviewerID" in df.columns:
        print("\nQ7. Top 10 most active reviewers")
        (
            df.groupBy("reviewerID")
            .agg(count("*").alias("reviews_written"))
            .orderBy(desc("reviews_written"))
            .show(10, truncate=False)
        )

    # 8) Average rating per product (top 10 by count)
    if "asin" in df.columns and "overall" in df.columns:
        print("\nQ8. Average rating per product (top 10 by review count)")
        (
            df.groupBy("asin")
            .agg(
                count("*").alias("review_count"),
                avg("overall").alias("avg_rating")
            )
            .orderBy(desc("review_count"))
            .show(10, truncate=False)
        )

    # 9) Review length statistics
    if "reviewText" in df.columns:
        print("\nQ9. Review text length statistics")
        df_len = df.withColumn("review_len", length(col("reviewText")))
        df_len.select(
            avg("review_len").alias("avg_review_length"),
            spark_min("review_len").alias("min_review_length"),
            spark_max("review_len").alias("max_review_length")
        ).show(truncate=False)

    # 10) Reviews by year/month
    if "review_year" in df.columns and "review_month" in df.columns:
        print("\nQ10. Reviews trend by year and month")
        (
            df.groupBy("review_year", "review_month")
            .agg(count("*").alias("review_count"))
            .orderBy("review_year", "review_month")
            .show(100, truncate=False)
        )


def main():
    spark = create_spark_session()

    print(f"Loading data from: {INPUT_PATH}")
    df = load_data(spark, INPUT_PATH)

    basic_overview(df)

    df_clean = preprocess(df)
    print("\n=== SAMPLE DATA (CLEANED) ===")
    df_clean.show(10, truncate=False)

    run_queries(df_clean)

    spark.stop()


if __name__ == "__main__":
    main()
