import os
from google.cloud import storage
from google.oauth2 import service_account
import config
import logging

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_gcs_client():
    """Tạo kết nối tới GCS bằng service account"""
    try:
        credentials = service_account.Credentials.from_service_account_file(config.GCP_KEY_PATH)
        client = storage.Client(credentials=credentials)
        return client
    except Exception as e:
        logger.error(f"Failed to create GCS client: {e}")
        raise

def upload_to_gcs(local_file_path, destination_blob_name):
    """
    Docstring for upload_to_gcs
    
    :param local_file_path: File csv
    :param destination_blob_name: seller_reports/
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        logger.info(f"Uploading {local_file_path} to gs://{config.GCS_BUCKET_NAME}/{destination_blob_name} ...")
        blob.upload_from_filename(local_file_path)

        logger.info("Upload successful!")
        return True
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return False
    
if __name__ == "__main__":
    with open("text.txt", "w") as f:
        f.write("Hello GCS")
    upload_to_gcs("test.txt", "test_folder/text.txt")