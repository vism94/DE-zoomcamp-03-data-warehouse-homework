CREATE OR REPLACE EXTERNAL TABLE `kestra-orchestration-486011.warehouse_dataset.external_yellow_tripdata`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://warehouse-lesson-bucket/yellow_tripdata_2024-*.parquet']);

SELECT *
FROM kestra-orchestration-486011.warehouse_dataset.external_yellow_tripdata
LIMIT 10;

CREATE OR REPLACE TABLE `kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_non_partitioned`
AS
SELECT *
FROM kestra-orchestration-486011.warehouse_dataset.external_yellow_tripdata;

CREATE OR REPLACE TABLE `kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_partitioned`
  PARTITION BY DATE(tpep_pickup_datetime)
AS
SELECT *
FROM kestra-orchestration-486011.warehouse_dataset.external_yellow_tripdata;

SELECT COUNT(*)
FROM
  kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_non_partitioned;

SELECT COUNT(*)
FROM
  kestra-orchestration-486011.warehouse_dataset.external_yellow_tripdata;

SELECT COUNT(*) AS zero_fare_cnt
FROM kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_partitioned
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_partitioned_and_clustered`
  PARTITION BY date(tpep_dropoff_datetime)
  CLUSTER BY VendorID
AS
SELECT *
FROM kestra-orchestration-486011.warehouse_dataset.external_yellow_tripdata;

SELECT DISTINCT VendorID
FROM
  kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_non_partitioned
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM
  kestra-orchestration-486011.warehouse_dataset.yellow_tripdata_partitioned_and_clustered
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
