from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

def init_minio_client():

    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        region=os.getenv("MINIO_REGION"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
    )

    bucket_name = os.getenv("MINIO_BUCKET")

    found = client.bucket_exists(bucket_name)
    print("Found bucket:", found)
    if not found:
        client.make_bucket(bucket_name)
        
    return client, bucket_name