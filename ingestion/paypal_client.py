# ingestion/paypal_client.py
# Fetches order data from PayPal Sandbox using Orders API
# Uses /v2/checkout/orders which works with basic sandbox permissions

import requests
import json
from datetime import datetime, timezone
from ingestion.paypal_auth import get_access_token
from config.settings import PAYPAL_BASE_URL


def get_order_details(order_id: str, token: str) -> dict:
    """
    Fetches details of a single order by ID.

    Args:
        order_id: PayPal order ID e.g. "4TG12345XX"
        token: Bearer token from OAuth2

    Returns:
        Order dict with full details
    """
    url = f"{PAYPAL_BASE_URL}/v2/checkout/orders/{order_id}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_all_orders() -> list:
    """
    Reads order IDs from our local sandbox_orders.json
    and fetches full details for each from PayPal API.

    Returns:
        List of order dicts with full PayPal data
    """
    token = get_access_token()

    # Load order IDs we created earlier
    try:
        with open("data/sandbox_orders.json", "r") as f:
            saved_orders = json.load(f)
    except FileNotFoundError:
        print("[ERROR] data/sandbox_orders.json not found!")
        print("        Run: python -m ingestion.create_sandbox_transactions first")
        return []

    print(f"[FETCH] Found {len(saved_orders)} orders to fetch...")

    all_orders = []

    for i, saved in enumerate(saved_orders, 1):
        order_id = saved["order_id"]
        try:
            details = get_order_details(order_id, token)

            # Extract the key fields we care about
            purchase_unit = details.get("purchase_units", [{}])[0]
            amount_info   = purchase_unit.get("amount", {})

            order = {
                "order_id":    details.get("id"),
                "status":      details.get("status"),
                "currency":    amount_info.get("currency_code"),
                "amount":      amount_info.get("value"),
                "description": purchase_unit.get("description", "N/A"),
                "created_at":  details.get("create_time"),
                "updated_at":  details.get("update_time"),
                "raw":         details
            }

            all_orders.append(order)
            print(f"[{i:02d}] ✅ Fetched | {order['description']:<30} | ${order['amount']} | {order['status']}")

        except Exception as e:
            print(f"[{i:02d}] ❌ Failed  | Order: {order_id} | Error: {e}")

    print(f"\n[FETCH] Total orders fetched: {len(all_orders)}")
    return all_orders


if __name__ == "__main__":
    orders = get_all_orders()

    if orders:
        print("\n📦 Sample order (first record):")
        sample = {k: v for k, v in orders[0].items() if k != "raw"}
        print(json.dumps(sample, indent=2))
