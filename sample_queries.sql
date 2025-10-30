-- Top 10 days by total tests
SELECT test_date, total_tests, error_rate
FROM healthcare.gold_daily_kpis
ORDER BY total_tests DESC
LIMIT 10;

-- Devices with highest error rates (min tests threshold)
SELECT device_id, device_type, tests_count, error_rate
FROM healthcare.gold_device_metrics
WHERE tests_count >= 50
ORDER BY error_rate DESC
LIMIT 10;

-- Patient-level recent tests
SELECT patient_id, name, test_timestamp, status, test_value
FROM healthcare.silver_patient_test_events
WHERE patient_id = 123
ORDER BY test_timestamp DESC
LIMIT 20;
