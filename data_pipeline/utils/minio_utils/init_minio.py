import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

def init_minio_client():
    """
    Initialize a MinIO client and ensure the specified bucket exists.

    This function loads environment variables to configure a MinIO client,
    checks if the specified bucket exists, and creates it if it does not.
    It validates that all required environment variables are set.

    Returns:
        tuple: (MinIO client instance, bucket name)

    Raises:
        ValueError: If any required environment variable is not set.
        S3Error: If an error occurs while interacting with MinIO (e.g., connection failure).
    """
    # Load environment variables
    load_dotenv()

    # Required environment variables
    required_vars = ["MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BUCKET"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Optional environment variables with defaults
    region = os.getenv("MINIO_REGION", None)  # Region is optional
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    try:
        # Initialize MinIO client
        client = Minio(
            endpoint=os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            region=region,
            secure=secure
        )

        # Get bucket name
        bucket_name = os.getenv("MINIO_BUCKET")

        # Check if bucket exists
        if client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            # Create bucket if it does not exist
            client.make_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'.")

        return client, bucket_name

    except S3Error as e:
        # Handle MinIO-specific errors (e.g., connection failure, access denied)
        print(f"Error initializing MinIO client: {e}")
        raise
    except Exception as e:
        # Handle unexpected errors
        print(f"Unexpected error initializing MinIO client: {e}")
        raise