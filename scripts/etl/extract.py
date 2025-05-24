from scripts.utils.minio_utils import init_minio_client

def get_unprocessed_files(client, bucket_name, source, date_str):
    print(f"Đang kiểm tra các file chưa được xử lý trong bucket {bucket_name} với prefix {source}/{date_str}/")
    prefix = f"raw/{source}/{date_str}/"
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    raw_files = [obj.object_name.split("/")[-1] for obj in objects if obj.object_name.endswith(".json")]

    unprocessed_files = []
    for file_name in raw_files:
        processed_prefix = f"processed/restaurants_unified/{date_str}/{file_name}/"
        processed_objects = list(client.list_objects(bucket_name, prefix=processed_prefix, recursive=True))
        
        if processed_objects:
            print(f"File {file_name} đã được xử lý (tìm thấy {len(processed_objects)} object).")
        else:
            print(f"File {file_name} chưa được xử lý.")
            unprocessed_files.append(file_name)

    return unprocessed_files