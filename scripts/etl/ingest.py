import os
from dotenv import load_dotenv
from scripts.utils.minio_utils import init_minio_client, upload_to_minio

def ingest_data_to_minio(raw_data_dir="data/raw"):
    """
    Ingest raw data files from a local directory to MinIO, overwriting any existing files.
    
    Args:
        raw_data_dir (str): Path to the directory containing raw data files.
    """
    # Load environment variables
    load_dotenv()

    # Initialize MinIO client
    client, bucket_name = init_minio_client()

    print("Starting data upload to MinIO")

    # Check if directory exists
    if not os.path.exists(raw_data_dir):
        print(f"Directory {raw_data_dir} does not exist.")
        return

    # Iterate through files in the directory
    for filename in os.listdir(raw_data_dir):
        local_path = os.path.join(raw_data_dir, filename)
        if os.path.isfile(local_path):  # Ensure it's a file, not a directory
            upload_to_minio(client, bucket_name, local_path)

if __name__ == "__main__":
    ingest_data_to_minio()