# ingestion/upload_to_adls.py
# Uploads raw JSON data to Azure Data Lake (Bronze layer)

import json
from datetime import datetime
from config.azure_config import get_adls_client
from config.settings import AZURE_CONTAINER_NAME

def upload_json_to_bronze(data: list, filename: str = None) -> str:
    """
    Uploads raw transaction JSON to ADLS Bronze layer.

    Args:
        data: List of transaction dicts
        filename: Optional custom filename

    Returns:
        Full path of uploaded file
    """
    client = get_adls_client()
    fs_client = client.get_file_system_client(AZURE_CONTAINER_NAME)

    # Partition by date: bronze/transactions/year=2024/month=01/
    now = datetime.utcnow()
    folder = f"transactions/year={now.year}/month={now.month:02d}"
    filename = filename or f"transactions_{now.strftime('%Y%m%d_%H%M%S')}.json"
    file_path = f"{folder}/{filename}"

    # Upload
    file_client = fs_client.get_file_client(file_path)
    file_client.upload_data(json.dumps(data, indent=2), overwrite=True)

    print(f"[UPLOAD] Uploaded {len(data)} records to: {file_path}")
    return file_path


if __name__ == "__main__":
    # Test with dummy data
    sample = [{"id": "test_001", "amount": "99.99", "currency": "USD"}]
    path = upload_json_to_bronze(sample, "test_upload.json")
    print(f"Uploaded to: {path}")
