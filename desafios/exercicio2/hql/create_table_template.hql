CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ({columns})
STORED AS PARQUET
LOCATION 's3://iti-query-results/'