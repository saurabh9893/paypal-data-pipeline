# ingestion/create_sandbox_transactions.py
# Creates fake PayPal transactions in Sandbox for testing our pipeline
# Run: python -m ingestion.create_sandbox_transactions

import requests
import json
import time
from ingestion.paypal_auth import get_access_token
from config.settings import PAYPAL_BASE_URL

# --- Fake order data to simulate real e-commerce ---
SAMPLE_ORDERS = [
    {"amount": "29.99",  "currency": "USD", "description": "Nike Running Shoes"},
    {"amount": "149.99", "currency": "USD", "description": "Apple AirPods"},
    {"amount": "9.99",   "currency": "USD", "description": "Netflix Subscription"},
    {"amount": "79.00",  "currency": "USD", "description": "Levi's Jeans"},
    {"amount": "5.49",   "currency": "USD", "description": "Starbucks Coffee"},
    {"amount": "299.99", "currency": "USD", "description": "Samsung Tablet"},
    {"amount": "19.99",  "currency": "USD", "description": "Spotify Premium"},
    {"amount": "55.00",  "currency": "USD", "description": "Zara Jacket"},
    {"amount": "12.99",  "currency": "USD", "description": "Book: Clean Code"},
    {"amount": "499.00", "currency": "USD", "description": "Sony Headphones"},
]


def create_order(token: str, order: dict) -> dict:
    """
    Creates a single fake PayPal order.

    PayPal Orders API creates an order in 'CREATED' status.
    In production this would be approved by a buyer.
    For our pipeline, CREATED status is enough to appear in reports.
    """
    url = f"{PAYPAL_BASE_URL}/v2/checkout/orders"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "amount": {
                    "currency_code": order["currency"],
                    "value": order["amount"]
                },
                "description": order["description"]
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


def create_all_transactions():
    """Creates all sample orders and prints a summary."""
    print("=" * 50)
    print("  Creating Fake PayPal Sandbox Transactions")
    print("=" * 50)

    token = get_access_token()
    created = []

    for i, order in enumerate(SAMPLE_ORDERS, 1):
        try:
            result = create_order(token, order)
            order_id = result.get("id", "N/A")
            status   = result.get("status", "N/A")

            created.append({
                "order_id":    order_id,
                "description": order["description"],
                "amount":      f"${order['amount']} {order['currency']}",
                "status":      status
            })

            print(f"[{i:02d}] ✅ Created | {order['description']:<30} | ${order['amount']} | ID: {order_id}")

            # Small delay to avoid rate limiting
            time.sleep(0.5)

        except Exception as e:
            print(f"[{i:02d}] ❌ Failed  | {order['description']:<30} | Error: {e}")

    print("\n" + "=" * 50)
    print(f"  Done! Created {len(created)}/{len(SAMPLE_ORDERS)} transactions")
    print("=" * 50)

    # Save to local JSON for reference
    with open("data/sandbox_orders.json", "w") as f:
        json.dump(created, f, indent=2)
    print("\n📁 Saved to: data/sandbox_orders.json")

    return created


if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)
    create_all_transactions()
