import os
import logging
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_gcs_client():
    """Tạo kết nối tới GCS bằng Service Account"""
    try:
        credentials = service_account.Credentials.from_service_account_file(config.GCP_KEY_PATH)
        client = storage.Client(credentials=credentials)
        return client
    except Exception as e:
        logger.error(f"Failed to create GCS client: {e}")
        raise

def get_bq_client():
    """Tạo kết nối tới BigQuery bằng Service Account"""
    try:
        credentials = service_account.Credentials.from_service_account_file(config.GCP_KEY_PATH)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return client
    except Exception as e:
        logger.error(f"Failed to create BigQuery client: {e}")
        raise

def upload_to_gcs(local_file_path, destination_blob_name):
    """
    Upload file từ máy local lên Google Cloud Storage
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        logger.info(f"Uploading {local_file_path} to gs://{config.GCS_BUCKET_NAME}/{destination_blob_name}...")
        
        blob.upload_from_filename(local_file_path)
        
        logger.info("Upload to GCS successful!")
        return True
    except Exception as e:
        logger.error(f"GCS Upload failed: {e}")
        return False

def create_bq_dataset(dataset_name, location="US"):
    """
    Tạo BigQuery Dataset nếu chưa tồn tại
    """
    client = get_bq_client()
    dataset_id = f"{client.project}.{dataset_name}"

    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)
        logger.info(f"Created new dataset: {dataset_id} in {location}")

def load_gcs_to_bigquery(gcs_uri, dataset_id, table_id):
    """
    Load file từ GCS vào BigQuery
    :param gcs_uri: Đường dẫn file trên GCS 
    :param dataset_id: Tên dataset 
    :param table_id: Tên bảng muốn tạo 
    """
    try:
        client = get_bq_client()

        # Cấu hình job load
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1, 
            autodetect=True,     
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Xóa cũ ghi mới
        )

        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        
        logger.info(f"Loading {gcs_uri} into BigQuery table {table_ref}...")
        
        load_job = client.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )

        load_job.result()  
        logger.info(f"BigQuery Load Success! Loaded {load_job.output_rows} rows into {table_id}.")
        return True
        
    except Exception as e:
        logger.error(f"BigQuery Load failed: {e}")
        return False

