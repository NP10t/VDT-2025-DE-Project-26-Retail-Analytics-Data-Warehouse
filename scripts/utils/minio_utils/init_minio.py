from minio import Minio
from dotenv import load_dotenv
import os

# load_dotenv()

def init_minio_client():
    
    print("Initializing MinIO client...")
    print("Environment variables:")
    print("MINIO_ENDPOINT:", os.getenv("MINIO_ENDPOINT"))
    print("MINIO_ROOT_USER:", os.getenv("MINIO_ROOT_USER"))
    print("MINIO_ROOT_PASSWORD:", os.getenv("MINIO_ROOT_PASSWORD"))
    print("MINIO_REGION:", os.getenv("MINIO_REGION"))
    print("MINIO_SECURE:", os.getenv("MINIO_SECURE"))
    print("MINIO_BUCKET:", os.getenv("MINIO_BUCKET"))

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