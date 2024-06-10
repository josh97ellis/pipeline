import polars as pl
from azure.storage.blob import BlobServiceClient
import io


def upload_dataframe_to_blob(df: pl.DataFrame, account_name, account_key, container_name, blob_name, file_format='json'):
    """
    Upload a Polars DataFrame to Azure Blob Storage as a CSV or Parquet file.

    Parameters:
    - df (pl.DataFrame): The Polars DataFrame to upload.
    - account_name (str): Azure Storage Account name.
    - account_key (str): Azure Storage Account key.
    - container_name (str): Blob container name.
    - blob_name (str): Blob name, include the file extension in the name like 'data.csv' or 'data.parquet'.
    - file_format (str): Format of the file to upload ('csv' or 'parquet').
    """
    # Serialize DataFrame to the specified format
    if file_format == 'csv':
        data = df.with_columns(pl.exclude(pl.Utf8).cast(str)).write_csv().encode('utf-8')  # Convert DataFrame to CSV and then to bytes
    elif file_format == 'json':
        data = df.with_columns(pl.exclude(pl.Utf8).cast(str)).write_json(row_oriented=True)
    elif file_format == 'parquet':
        buffer = io.BytesIO()
        data = df.write_parquet(buffer)
        data = buffer.getbuffer()  # Get bytes value of buffer
    else:
        raise ValueError("Unsupported file format. Choose 'csv', 'json', or 'parquet'.")

    # Create a BlobServiceClient object
    connection_string = f""
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a client to interact with the specified container
    container_client = blob_service_client.get_container_client(container_name)

    # Get a client to interact with the specified blob
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the DataFrame to the blob
    blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)

    print(f"Data uploaded successfully to Azure Blob Storage as {blob_name}.")
