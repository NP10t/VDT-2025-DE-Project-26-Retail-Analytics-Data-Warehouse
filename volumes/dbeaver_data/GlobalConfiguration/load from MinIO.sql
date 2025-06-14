INSERT INTO silver
SELECT *
FROM s3(
    minio_config,
    url = 'http://minio:9000/vdt-data/cleaned_raw/retail_cleaned/*.parquet',
    format = 'Parquet'
);


SELECT *
FROM s3(
    url = 'http://minio:9000/vdt-data/cleaned_raw/retail_cleaned/*.parquet', 'Parquet'
);