# config/azure_config.py
# Azure Data Lake Storage Gen2 connection helper

from azure.storage.filedatalake import DataLakeServiceClient
from config.settings import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY

def get_adls_client() -> DataLakeServiceClient:
    """Returns an authenticated ADLS Gen2 client."""
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(
        account_url=account_url,
        credential=AZURE_STORAGE_KEY
    )
