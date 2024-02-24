#%%
import pandas as pd
import numpy as np
from google.cloud import storage

# load data
df = pd.read_parquet("data/milano_pollution.parquet")
station_to_keep = df["Station"].unique()[0:5]
df = df[df["Station"].isin(station_to_keep)]

# replcae strings with floats
for col in df.columns: 
    if col != "Station" and col != "Date":
        print(col)
        df[col] = df[col].apply(lambda x: 0. if x == "< 0.5" else x)
        df[col] = df[col].apply(lambda x: 5. if x == "< 5" else x)
        df[col] = df[col].apply(lambda x: 2. if x == "< 2" else x)
        df[col] = df[col].apply(lambda x: .4 if x == "< 4" else x)
        df[col] = df[col].apply(lambda x: 1 if x == "< 1" else x)
        df[col] = df[col].apply(lambda x: np.nan if x == "-" else x)
        df[col] = df[col].apply(lambda x: np.nan if x == "N.D." else x)
        df[col] = df[col].str.replace(',', '.') if df[col].dtype == "object" else df[col]

# replace < 0.5 with 0.5
df['CO'] = df['CO'].apply(lambda x: 0.5 if x == "< 0.5" else x)

# df to float
for col in df.columns: 
    if col != "Station" and col != "Date":
        x = col
        df[col] = df[col].astype(float)

# datetime
df.Date = pd.to_datetime(df.Date)

# generate means
df = df.groupby("Date").mean(["SO2", "PM10", "PM2.5", "NO2", "CO", "O3", "C6H6"]).round(3)


df.plot(subplots=True, figsize=(15, 15))

# to parquet
df.to_parquet('data/milano_pollution_clean.parquet')
# %%



#%%


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
