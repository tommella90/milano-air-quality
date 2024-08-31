#%%
import pandas as pd
import numpy as np
from google.cloud import storage


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

# Example usage
bucket_name = "milano-data"
source_blob_name = "air-quality-clean.parquet"
destination_file_name = "data/milano_pollution_clean_bucket.parquet"

download_blob(bucket_name, source_blob_name, destination_file_name)
