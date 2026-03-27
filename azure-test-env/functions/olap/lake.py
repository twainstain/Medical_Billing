"""ADLS Gen2 Lakehouse helpers — read/write Parquet to Bronze/Silver/Gold containers.

Uses azure-storage-blob + pandas for serverless Medallion architecture
on Azure Functions consumption plan (no Spark/Fabric needed).
"""

import io
import os
import logging
from datetime import datetime

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient

logger = logging.getLogger(__name__)

_blob_service: BlobServiceClient = None


def _get_blob_service() -> BlobServiceClient:
    global _blob_service
    if _blob_service is None:
        conn_str = os.environ.get("STORAGE_CONNECTION_STRING")
        if not conn_str:
            account = os.environ.get("STORAGE_ACCOUNT", "")
            key = os.environ.get("STORAGE_KEY", "")
            conn_str = (
                f"DefaultEndpointsProtocol=https;AccountName={account};"
                f"AccountKey={key};EndpointSuffix=core.windows.net"
            )
        _blob_service = BlobServiceClient.from_connection_string(conn_str)
    return _blob_service


def _container(name: str) -> ContainerClient:
    return _get_blob_service().get_container_client(name)


def write_parquet(container: str, path: str, df: pd.DataFrame):
    """Write a DataFrame as Parquet to ADLS Gen2."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob = _container(container).get_blob_client(path)
    blob.upload_blob(buf, overwrite=True)
    logger.info("Wrote %d rows to %s/%s", len(df), container, path)


def read_parquet(container: str, path: str) -> pd.DataFrame:
    """Read a single Parquet file from ADLS Gen2."""
    blob = _container(container).get_blob_client(path)
    data = blob.download_blob().readall()
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def read_parquet_folder(container: str, prefix: str) -> pd.DataFrame:
    """Read all Parquet files under a prefix, concatenate into one DataFrame."""
    dfs = []
    for blob in _container(container).list_blobs(name_starts_with=prefix):
        if blob.name.endswith(".parquet"):
            try:
                df = read_parquet(container, blob.name)
                dfs.append(df)
            except Exception as e:
                logger.warning("Failed to read %s: %s", blob.name, e)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def list_parquet_files(container: str, prefix: str) -> list:
    """List Parquet file paths under a prefix."""
    return [
        blob.name
        for blob in _container(container).list_blobs(name_starts_with=prefix)
        if blob.name.endswith(".parquet")
    ]


def parquet_path(table: str) -> str:
    """Standard path for a lakehouse table: {table}/current.parquet"""
    return f"{table}/current.parquet"


def parquet_path_dated(table: str) -> str:
    """Date-partitioned path: {table}/YYYY/MM/DD/{timestamp}.parquet"""
    now = datetime.utcnow()
    return f"{table}/{now:%Y}/{now:%m}/{now:%d}/{now:%Y%m%d%H%M%S}.parquet"
