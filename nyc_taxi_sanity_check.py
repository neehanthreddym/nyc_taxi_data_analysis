import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, when, mean, count, sum as spark_sum

def main():
    parser = argparse.ArgumentParser(description="NYC Taxi sanity check and mini analysis")
    parser.add_argument("--input", required=True, help="Path to input CSV (NYC taxi-like schema)")
    parser.add_argument("--output", required=True, help="Output directory to write results")
    args = parser.parse_args()

    t0 = time.time()

    spark = SparkSession.builder.appName("NYC_Taxi_Sanity_Check").getOrCreate()

    # Read input data
    if args.input.endswith(".parquet"):
        print(f"Reading Parquet file: {args.input}")
        df = spark.read.parquet(args.input)
    else:
        print(f"Reading CSV file: {args.input}")
        df = spark.read.csv(args.input, header=True, inferSchema=True)
    t_read = time.time()

    # Basic counts
    total_rows = df.count()

    # Null checks for key fields
    key_cols = ["tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance","fare_amount","passenger_count"]
    null_counts = {c: df.filter(col(c).isNull()).count() for c in key_cols}

    # Cleaning: remove invalid distance/fare and nonsensical passenger counts
    df_clean = (
        df
        .filter((col("trip_distance") > 0) & (col("fare_amount") >= 0))
        .filter((col("passenger_count") >= 1) & (col("passenger_count") <= 8))
    )

    # Compute trip duration (minutes) and average speed (mph)
    df_enriched = (
        df_clean
        .withColumn("pickup_ts", unix_timestamp(col("tpep_pickup_datetime")))
        .withColumn("dropoff_ts", unix_timestamp(col("tpep_dropoff_datetime")))
        .withColumn("duration_min", (col("dropoff_ts") - col("pickup_ts")) / 60.0)
        .withColumn("duration_hr", (col("dropoff_ts") - col("pickup_ts")) / 3600.0)
        .withColumn("avg_mph", when(col("duration_hr") > 0, col("trip_distance") / col("duration_hr")).otherwise(None))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .filter((col("avg_mph") <= 80) & (col("avg_mph").isNotNull()))
    )
    t_transform = time.time()

    # Aggregates: by hour
    by_hour = (
        df_enriched
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            mean("duration_min").alias("avg_duration_min"),
            mean("avg_mph").alias("avg_speed_mph"),
            mean("fare_amount").alias("avg_fare"),
        )
        .orderBy("pickup_hour")
    )

    # Aggregates: passenger patterns
    by_passenger = (
        df_enriched
        .groupBy("passenger_count")
        .agg(
            count("*").alias("trip_count"),
            mean("trip_distance").alias("avg_distance"),
            (spark_sum("fare_amount")/spark_sum("trip_distance")).alias("fare_per_mile")
        )
        .orderBy("passenger_count")
    )

    t_agg = time.time()

    # Write outputs (coalesce to single files for easy grading)
    by_hour.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "/by_hour")
    by_passenger.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "/by_passenger")

    timings = {
        "total_rows_read": total_rows,
        "null_counts": null_counts,
        "timings_sec": {
            "read": round(t_read - t0, 4),
            "transform": round(t_transform - t_read, 4),
            "aggregate": round(t_agg - t_transform, 4),
            "total": round(time.time() - t0, 4)
        }
    }

    # Save timings.json to local fs (driver)
    import os
    os.makedirs(args.output, exist_ok=True)
    with open(os.path.join(args.output, "timings.json"), "w") as f:
        json.dump(timings, f, indent=2)

    print("=== SUMMARY ===")
    print(json.dumps(timings, indent=2))

    spark.stop()

if __name__ == "__main__":
    main()
