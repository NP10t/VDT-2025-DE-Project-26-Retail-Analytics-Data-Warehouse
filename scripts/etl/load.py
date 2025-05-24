import os
from pyspark.sql.functions import col, explode

def save_processed_data_to_minIO(spark, bucket_name, df_unified):
    output_path = f"s3a://{bucket_name}/processed/restaurants_unified/"
    
    df_unified.write \
        .format("json") \
        .mode("append") \
        .save(f"{output_path}")

def save_processed_data_to_local(client, bucket_name, prefix, local_dir):
    if os.path.exists(local_dir):
        if os.path.isfile(local_dir):
            os.remove(local_dir)

    os.makedirs(local_dir, exist_ok=True)

    # Lấy danh sách object
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)

    for obj in objects:
        if obj.object_name.endswith(".json"):
            filename = os.path.basename(obj.object_name)
            local_path = os.path.join(local_dir, filename)
            client.fget_object(bucket_name, obj.object_name, local_path)
            print(f"Downloaded: {local_path}")

def write_to_postgres(df_unified, postgres_config):
    # Triển khai logic ghi vào PostgreSQL (giả định)
    df_unified.write \
        .format("jdbc") \
        .option("url", postgres_config["url"]) \
        .option("dbtable", "restaurants") \
        .option("user", postgres_config["user"]) \
        .option("password", postgres_config["password"]) \
        .mode("append") \
        .save()