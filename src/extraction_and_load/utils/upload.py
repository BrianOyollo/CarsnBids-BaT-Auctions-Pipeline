import time
from datetime import datetime, timedelta
import json
import os
import io
import boto3
import pandas as pd
from .ntfy import send_notification

raw_auctions_bucket = os.getenv("DBT_ENV_S3_BUCKET")
ntfy_topic = os.getenv("NTFY_TOPIC")

def upload_to_s3(s3_client, auction_data:list[dict]):

    """ Uploads auction data to an S3 bucket in Parquet format.

    This function converts a list of auction data dictionaries into a pandas DataFrame,
    writes it to an in-memory Parquet file, and uploads the file to an S3 bucket using
    the provided Boto3 S3 client.

    The file is stored under the key:
        carsnbids/<yesterday's date>.parquet

    Parameters
    ----------
    s3_client : boto3.client
        An initialized Boto3 S3 client used to perform the upload.
    
    auction_data : list[dict]
        A list of dictionaries containing auction data. Each dictionary represents one auction record.

    Behavior
    --------
    - Converts `auction_data` into a DataFrame.
    - Writes the DataFrame to Parquet format using pyarrow with snappy compression.
    - Uploads the resulting binary data to S3 using `put_object`.
    - Automatically uses yesterday’s date as the object key (e.g., carsnbids/2025-11-01.parquet).
    - Sends a notification if the upload fails.

    """

    buffer = io.BytesIO()
    df = pd.DataFrame(auction_data)
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)

    key = (datetime.now().date()-timedelta(days=1)).isoformat()

    try:
        s3_client.put_object(
            Bucket = raw_auctions_bucket,
            Key = f'carsnbids/{key}.parquet',
            Body = buffer.getvalue()
        )
        
    except Exception as e:
        send_notification(ntfy_topic,"Error uploading file to s3")
