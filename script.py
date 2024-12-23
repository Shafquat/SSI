import os
import boto3
from botocore.exceptions import NoCredentialsError

def get_existing_files(s3_client, bucket_name):
    """Retrieve the list of existing files in the S3 bucket."""
    existing_files = set()
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    existing_files.add(obj['Key'])
    except Exception as e:
        print(f"Error fetching existing files: {e}")
    return existing_files

def upload_files_to_s3(local_folder, bucket_name, aws_access_key, aws_secret_key):
    """Uploads files from the local folder to the specified S3 bucket."""
    # Create an S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Get the list of existing files in the bucket
    existing_files = get_existing_files(s3_client, bucket_name)

    # Iterate over the local folder
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)

            if relative_path not in existing_files:
                try:
                    print(f"Uploading {relative_path}...")
                    s3_client.upload_file(local_file_path, bucket_name, relative_path)
                    print(f"Uploaded: {relative_path}")
                except FileNotFoundError:
                    print(f"File not found: {local_file_path}")
                except NoCredentialsError:
                    print("AWS credentials not available.")
                except Exception as e:
                    print(f"Failed to upload {relative_path}: {e}")
            else:
                print(f"File already exists in S3: {relative_path}")

if __name__ == "__main__":
    LOCAL_FOLDER = "D:/division_data"
    BUCKET_NAME = "dataproject-redshifttempdatabucket-s6sp8tfn9hgn"
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    upload_files_to_s3(LOCAL_FOLDER, BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY)
