# notebooks/3_aggregate_gold.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, sum as _sum, expr
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("GoldAggregate").getOrCreate()

silver_base = "/mnt/silver/healthcare"
gold_base = "/mnt/gold/healthcare"

events = spark.read.format("delta").load(silver_base + "/patient_test_events")

# Example Gold aggregations:
# 1) Daily KPIs
daily_kpis = events.withColumn("test_date", to_date(col("test_timestamp"))) \
    .groupBy("test_date") \
    .agg(
        count("*").alias("total_tests"),
        _sum((col("status") != "OK").cast("int")).alias("total_errors"),
        avg("test_duration_seconds").alias("avg_test_duration"),
        avg("test_value").alias("avg_test_value")
    ) \
    .withColumn("error_rate", expr("total_errors / total_tests"))

# 2) Device-level metrics
device_metrics = events.groupBy("device_id", "device_type", "location") \
    .agg(
        count("*").alias("tests_count"),
        _sum((col("status") != "OK").cast("int")).alias("errors"),
        avg("test_duration_seconds").alias("avg_duration")
    ).withColumn("error_rate", expr("errors / tests_count"))

# Write Gold Delta
daily_kpis.write.format("delta").mode("overwrite").save(gold_base + "/daily_kpis")
device_metrics.write.format("delta").mode("overwrite").save(gold_base + "/device_metrics")

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_daily_kpis USING DELTA LOCATION '{gold_base}/daily_kpis'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_device_metrics USING DELTA LOCATION '{gold_base}/device_metrics'")

print("Gold aggregation complete.")
