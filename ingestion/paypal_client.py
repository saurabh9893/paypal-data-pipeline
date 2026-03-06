# ingestion/paypal_client.py
# Fetches transaction data from PayPal Sandbox API

import requests
import json
from datetime import datetime, timedelta
from ingestion.paypal_auth import get_access_token
from config.settings import PAYPAL_BASE_URL

def get_transactions(start_date: str = None, end_date: str = None) -> list:
    """
    Fetches transactions from PayPal Reporting API.

    Args:
        start_date: ISO format e.g. "2024-01-01T00:00:00-0700"
        end_date:   ISO format e.g. "2024-01-31T23:59:59-0700"

    Returns:
        List of transaction dicts
    """
    token = get_access_token()

    # Default: last 30 days
    if not start_date:
        start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S-0000")
    if not end_date:
        end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S-0000")

    url = f"{PAYPAL_BASE_URL}/v1/reporting/transactions"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "fields": "all",
        "page_size": 100,
        "page": 1
    }

    all_transactions = []

    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        transactions = data.get("transaction_details", [])
        all_transactions.extend(transactions)

        # Pagination
        total_pages = data.get("total_pages", 1)
        if params["page"] >= total_pages:
            break
        params["page"] += 1
        print(f"[FETCH] Fetched page {params['page']-1}/{total_pages}")

    print(f"[FETCH] Total transactions fetched: {len(all_transactions)}")
    return all_transactions


if __name__ == "__main__":
    transactions = get_transactions()
    print(json.dumps(transactions[:2], indent=2))  # Preview first 2
