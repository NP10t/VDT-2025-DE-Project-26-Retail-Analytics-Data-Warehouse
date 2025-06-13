from minio.error import S3Error
import os

def upload_to_minio(client, bucket_name, local_path, prefix="raw"):
    """
    Upload a file to a MinIO bucket, checking for duplicates by object name.

    This function checks if a file already exists in the specified MinIO bucket under
    the given prefix (e.g., 'raw/', 'test/'). If the file exists, it skips the upload.
    If not, it uploads the file to the specified prefix directory in the bucket.

    Args:
        client (Minio): Initialized MinIO client instance.
        bucket_name (str): Name of the MinIO bucket to upload to.
        local_path (str): Full path to the local file to be uploaded.
        prefix (str): Directory prefix in the bucket (e.g., 'raw', 'test'). Default is 'raw'.

    Returns:
        bool: True if the file was uploaded successfully, False if skipped due to an existing file.

    Raises:
        S3Error: If an error occurs during interaction with MinIO, except for 'NoSuchKey' errors.
    """
    # Construct the object name by placing the file under the specified prefix
    object_name = f"{prefix.rstrip('/')}/{os.path.basename(local_path)}"

    try:
        # Check if the object already exists in the MinIO bucket
        client.stat_object(bucket_name, object_name)
        print(f"Object {object_name} already exists in bucket {bucket_name}, skipping upload.")
        return False
    except S3Error as e:
        if e.code == "NoSuchKey":
            # Upload the file to MinIO
            client.fput_object(bucket_name, object_name, local_path)
            print(f"Successfully uploaded {local_path} to s3a://{bucket_name}/{object_name}")
            return True
        else:
            # Re-raise unexpected MinIO errors
            raise e