-- create_delta_tables.sql
-- If you are using Spark SQL / Fabric SQL
CREATE SCHEMA IF NOT EXISTS healthcare;

-- Bronze tables (if not created by notebooks)
CREATE TABLE IF NOT EXISTS healthcare.bronze_test_logs
USING DELTA
LOCATION '/mnt/bronze/healthcare/test_logs';

CREATE TABLE IF NOT EXISTS healthcare.bronze_patients
USING DELTA
LOCATION '/mnt/bronze/healthcare/patients';

CREATE TABLE IF NOT EXISTS healthcare.bronze_devices
USING DELTA
LOCATION '/mnt/bronze/healthcare/devices';

-- Silver
CREATE TABLE IF NOT EXISTS healthcare.silver_patient_test_events
USING DELTA
LOCATION '/mnt/silver/healthcare/patient_test_events';

-- Gold
CREATE TABLE IF NOT EXISTS healthcare.gold_daily_kpis
USING DELTA
LOCATION '/mnt/gold/healthcare/daily_kpis';

CREATE TABLE IF NOT EXISTS healthcare.gold_device_metrics
USING DELTA
LOCATION '/mnt/gold/healthcare/device_metrics';
