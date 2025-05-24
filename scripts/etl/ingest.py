from minio import Minio
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()
from scripts.utils.minio_utils.init_minio import init_minio_client
from scripts.utils.minio_utils.up_load_to_minio import upload_to_minio

client, bucket_name = init_minio_client()

print("Start uploading data to MinIO")

# date_str_array = config.SELLECT_CRAWL_DATE_ARRAY
date_str_array = ["2025-05-11"]

for date_str in date_str_array:
    # Upload file ShopeeFood
    local_shopee_dir = f"data/raw/shopeefood/{date_str}"

    if os.path.exists(local_shopee_dir):
        for filename in os.listdir(local_shopee_dir):
            local_path = os.path.join(local_shopee_dir, filename)
            upload_to_minio(client, bucket_name, local_path, "shopeefood", date_str)
    else:
        print(f"Directory {local_shopee_dir} does not exist.")


    # Upload file Google Maps
    local_google_dir = f"data/raw/googlemaps/{date_str}"

    if os.path.exists(local_google_dir):
        for filename in os.listdir(local_google_dir):
            local_path = os.path.join(local_google_dir, filename)
            upload_to_minio(client, bucket_name, local_path, "googlemaps", date_str)
    else:
        print(f"Directory {local_google_dir} does not exist.")