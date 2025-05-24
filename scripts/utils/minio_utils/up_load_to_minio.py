import os

def upload_to_minio(client, bucket_name, local_path, source, date_str):
    object_name = f"raw/{source}/{date_str}/{os.path.basename(local_path)}"
    
    # Kiểm tra xem file đã tồn tại trên MinIO chưa
    try:
        client.stat_object(bucket_name, object_name)
        print(f"File {object_name} đã tồn tại trên MinIO, bỏ qua upload.")
        return False
    except Exception:
        # Upload file
        client.fput_object(bucket_name, object_name, local_path)
        print(f"Uploaded {local_path} to s3a://{bucket_name}/{object_name}")
        return True