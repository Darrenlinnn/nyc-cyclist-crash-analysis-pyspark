"""NYC cyclist crash analysis with PySpark.

Reads NYC motor vehicle collision records, keeps crashes that injured or killed
a cyclist, and answers: when, where and on which streets do they concentrate?

Usage:
    python src/crash_analysis.py --input data/bike_crashes.csv --out output
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_spark():
    return (SparkSession.builder
            .appName("nyc-cyclist-crash-analysis")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


def load_and_clean(spark, path):
    """Read raw collisions, fix types, and keep cyclist-harm crashes from 2020 on."""
    raw = spark.read.option("header", True).csv(path)
    df = (raw
          .withColumn("crash_ts", F.to_timestamp("crash_datetime"))
          .withColumn("cyclists_injured", F.col("cyclists_injured").cast("int"))
          .withColumn("cyclists_killed", F.col("cyclists_killed").cast("int"))
          # "NaN"/empty borough -> null, then title case for display
          .withColumn("borough", F.when(F.col("borough").isin("NaN", ""), None)
                                  .otherwise(F.initcap("borough")))
          .withColumn("street", F.upper(F.trim("on_street_name")))
          .withColumn("street", F.when(F.col("street").isin("NAN", ""), None).otherwise(F.col("street")))
          .dropDuplicates(["collision_id"])
          .where(F.col("crash_ts") >= F.lit("2020-01-01")))
    harmed = df.where((F.col("cyclists_injured") > 0) | (F.col("cyclists_killed") > 0))
    return df, harmed.withColumn("year", F.year("crash_ts")) \
                     .withColumn("hour", F.hour("crash_ts")) \
                     .withColumn("weekday", F.date_format("crash_ts", "E"))


def analyze(harmed):
    """Return a dict of result DataFrames."""
    out = {}
    # 1) Yearly trend
    out["by_year"] = (harmed.groupBy("year")
                      .agg(F.count("*").alias("crashes"),
                           F.sum("cyclists_injured").alias("injured"),
                           F.sum("cyclists_killed").alias("killed"))
                      .orderBy("year"))
    # 2) Hour of day x weekday (for a heatmap)
    out["by_hour_weekday"] = harmed.groupBy("weekday", "hour").agg(F.count("*").alias("crashes"))
    # 3) Borough totals
    out["by_borough"] = (harmed.where(F.col("borough").isNotNull())
                         .groupBy("borough").agg(F.count("*").alias("crashes"),
                                                 F.sum("cyclists_killed").alias("killed"))
                         .orderBy(F.desc("crashes")))
    # 4) Top 5 streets within each borough, using a window function
    w = Window.partitionBy("borough").orderBy(F.desc("crashes"))
    out["top_streets_by_borough"] = (harmed.where(F.col("borough").isNotNull() & F.col("street").isNotNull())
                                     .groupBy("borough", "street").agg(F.count("*").alias("crashes"))
                                     .withColumn("rank", F.row_number().over(w))
                                     .where(F.col("rank") <= 5)
                                     .orderBy("borough", "rank"))
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/bike_crashes.csv")
    p.add_argument("--out", default="output")
    args = p.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    all_crashes, harmed = load_and_clean(spark, args.input)
    harmed.cache()
    print(f"Collisions since 2020: {all_crashes.count():,}")
    print(f"Crashes injuring or killing a cyclist: {harmed.count():,}")
    harmed.agg(F.min("crash_ts").alias("first"), F.max("crash_ts").alias("last")).show(truncate=False)

    results = analyze(harmed)
    for name, df in results.items():
        df.write.mode("overwrite").parquet(f"{args.out}/{name}")   # columnar output
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.out}/{name}_csv")
    results["by_year"].show()
    results["by_borough"].show()
    results["top_streets_by_borough"].show(25, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
