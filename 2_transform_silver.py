# notebooks/1_ingest_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
spark = SparkSession.builder.appName("BronzeIngest").getOrCreate()

# Replace paths with your storage paths (DBFS or ADLS)
bronze_path = "/mnt/bronze/healthcare/test_logs"   # Delta location
patients_path = "/mnt/bronze/healthcare/patients"
devices_path = "/mnt/bronze/healthcare/devices"

csv_base = "/dbfs/FileStore/sample_output"  # example DBFS path; or use "abfss://..." for ADLS

# Read CSVs
test_logs_df = spark.read.option("header","true").option("inferSchema","true") \
    .csv(f"{csv_base}/test_logs.csv") \
    .withColumn("test_timestamp", to_timestamp(col("test_timestamp"), "yyyy-MM-dd HH:mm:ss"))

patients_df = spark.read.option("header","true").option("inferSchema","true").csv(f"{csv_base}/patients.csv")
devices_df = spark.read.option("header","true").option("inferSchema","true").csv(f"{csv_base}/devices.csv")

# Write as Delta (Bronze)
test_logs_df.write.format("delta").mode("overwrite").option("mergeSchema","true").save(bronze_path + "/test_logs")
patients_df.write.format("delta").mode("overwrite").option("mergeSchema","true").save(bronze_path + "/patients")
devices_df.write.format("delta").mode("overwrite").option("mergeSchema","true").save(bronze_path + "/devices")

# Register tables (optional)
spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_test_logs USING DELTA LOCATION '{bronze_path}/test_logs'")
spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_patients USING DELTA LOCATION '{bronze_path}/patients'")
spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_devices USING DELTA LOCATION '{bronze_path}/devices'")

print("Bronze ingestion complete.")
