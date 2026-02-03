import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import os
from pymongo import MongoClient
from datetime import datetime, timezone
from azure.core.exceptions import ResourceNotFoundError
import logging

MONGO_URI = os.getenv("MONGODB_URI")
AZURE_BLOB_CONN_STR = os.getenv("AZURE_BLOB_CONN_STR")
container_name="api-analytics"

def prepare_data(db_name,collection_name,
                 container_name='api-analytics',
                 now = datetime.now().replace(second=0, microsecond=0),
                 connection_string=AZURE_BLOB_CONN_STR):
    logging.info(f"Getting {collection_name} data")
    blob_service_client=BlobServiceClient.from_connection_string(connection_string)
    blob_path=collection_name
    client=MongoClient(MONGO_URI)

    # Try to download watermark; if not found => first run.
    try:
        blob_client=blob_service_client.get_blob_client(
            container=container_name,
            blob=f"{blob_path}/watermark/{blob_path}_timestamp.txt"
        )
        stream = blob_client.download_blob()
        watermark_str = stream.readall().decode("utf-8").strip()
        watermark_dt = None
        parse_error = None
        try:
            iso_text = watermark_str.replace("Z", "+00:00")
            watermark_dt = datetime.fromisoformat(iso_text)
        except Exception as e_iso:
            parse_error = e_iso
            try:
                watermark_dt = datetime.strptime(watermark_str, "%Y-%m-%d %H:%M:%S.%f")
            except Exception as e_dot:
                parse_error = e_dot
                try:
                    watermark_dt = datetime.strptime(watermark_str, "%Y-%m-%d %H:%M:%S%f")
                except Exception as e_nodot:

                    raise ValueError(f"Failed to parse watermark '{watermark_str}': {e_iso}; {e_dot}; {e_nodot}")


        if watermark_dt.tzinfo is None:
            watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
        else:
            watermark_dt = watermark_dt.astimezone(timezone.utc)

        # fetch only newer docs
        df=pd.DataFrame(list(client[f'{db_name}'][f'{collection_name}'].find({"createdAt":{"$gte":watermark_dt}})))
        logging.info(f"{len(df)} is the length")
        if len(df)!=0:
            logging.info(f"Loaded new {len(df)} Rows after date {watermark_dt}")
            logging.info(f"new max date is {df['createdAt'].max()}")
    except ResourceNotFoundError:
        # watermark not present => first time load
        logging.info("First Time Load (watermark not found)")
        df=pd.DataFrame(list(client[f'{db_name}'][f'{collection_name}'].find()))
    except Exception:
        raise

    if len(df)==0:
        logging.info("No new Data")
        return 0
    else:
        for i in df.columns:
            if i!='createdAt':
                df[i]=df[i].astype(str)
        df['load_time']=now    
        return df
    
    
def save_df_to_blob(df: pd.DataFrame, 
                    blob_path,
                    watermark='watermark',
                    container_name='api-analytics', 
                    connection_string: str=AZURE_BLOB_CONN_STR, 
                    partition_col: str = "load_time"):

    # Split container and blob name
    base_blob_path=blob_path+"/"+"data"
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    # Current datetime string (minute precision)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    if partition_col not in df.columns:
        raise ValueError(f"Partition column '{partition_col}' not found in DataFrame")

    for value, group in df.groupby(partition_col):
        # Serialize group to Parquet in memory
        buffer = BytesIO()
        group.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_name = f"{base_blob_path}/{partition_col}={value}/data_{timestamp}.parquet"
        blob_client = container_client.get_blob_client(blob_name)
        # allow overwrite to avoid ResourceExistsError on retry or rerun
        blob_client.upload_blob(buffer.read(), overwrite=True)

        logging.info(f" Saved partition '{value}' to blob: {blob_name}")
    if "createdAt" not in df.columns:
        raise ValueError("DataFrame must contain a 'createdAt' column for watermarking.")

    # Get max createdAt
    max_timestamp = df["createdAt"].max()

    # Ensure it's string-safe (ISO format if datetime)
    if isinstance(max_timestamp, (datetime, pd.Timestamp)):
        # write with dot and microseconds
        # normalize to UTC-aware
        if isinstance(max_timestamp, pd.Timestamp):
            max_ts_py = max_timestamp.to_pydatetime()
        else:
            max_ts_py = max_timestamp
        if max_ts_py.tzinfo is None:
            max_ts_py = max_ts_py.replace(tzinfo=timezone.utc)
        else:
            max_ts_py = max_ts_py.astimezone(timezone.utc)
        max_timestamp_str = max_ts_py.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        max_timestamp_str = str(max_timestamp)

    # Prepare watermark blob path
    watermark_blob_name = f"{blob_path}/{watermark}/{blob_path}_timestamp.txt"

    # Upload text file
    text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
    watermark_client = container_client.get_blob_client(watermark_blob_name)
    watermark_client.upload_blob(text_buffer.read(), overwrite=True)
    
def check_and_load_df(df,blob_path):
    logging.info(f"Handling {blob_path} data")
    if type(df)!= int:
        logging.info("Df is not empty")
        save_df_to_blob(df=df,blob_path=blob_path)
    else:
        logging.info("No Data to load")


        
#Lining up data 
def load_apeirosretaildataprocessing(db_name="apeirosretaildataprocessing"):
    for i in ["billRequest","billtransactions","invoiceExtractedData","receiptExtractedData"]:
        df=prepare_data(db_name=db_name,collection_name=i)
        check_and_load_df(df,i)

def load_apeirosretail(db_name="apeirosretail"):
    for i in ["storeDetails","organizationDetails","agentDetails","paymentDetails"]:
        df=prepare_data(db_name=db_name,collection_name=i)
        check_and_load_df(df,i)


#feedback
def get_feedback():        
    feedback_df=prepare_data(db_name="feedbackengine",collection_name="billFeedback")
    check_and_load_df(feedback_df,blob_path="billFeedback")

def main():
    load_apeirosretail()
    load_apeirosretaildataprocessing()
    get_feedback()

if __name__ == "__main__":
    main()