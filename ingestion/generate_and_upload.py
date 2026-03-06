# ingestion/generate_and_upload.py
# Generates 100k realistic PayPal orders using Faker
# and uploads them to Azure Data Lake Bronze layer
# Run: python -m ingestion.generate_and_upload

import json
import os
from datetime import datetime, timezone, timedelta
from faker import Faker
import random
from ingestion.upload_to_adls import upload_json_to_bronze

fake = Faker()

# --- Realistic e-commerce data ---
PRODUCTS = [
    {"name": "Nike Running Shoes",    "category": "Footwear",     "min": 49,  "max": 199},
    {"name": "Apple AirPods",         "category": "Electronics",  "min": 99,  "max": 249},
    {"name": "Netflix Subscription",  "category": "Subscription", "min": 9,   "max": 19},
    {"name": "Levi's Jeans",          "category": "Clothing",     "min": 49,  "max": 99},
    {"name": "Starbucks Coffee",      "category": "Food",         "min": 4,   "max": 12},
    {"name": "Samsung Tablet",        "category": "Electronics",  "min": 199, "max": 599},
    {"name": "Spotify Premium",       "category": "Subscription", "min": 9,   "max": 15},
    {"name": "Zara Jacket",           "category": "Clothing",     "min": 49,  "max": 149},
    {"name": "Book: Clean Code",      "category": "Books",        "min": 9,   "max": 39},
    {"name": "Sony Headphones",       "category": "Electronics",  "min": 99,  "max": 499},
    {"name": "Adidas T-Shirt",        "category": "Clothing",     "min": 19,  "max": 59},
    {"name": "iPhone Case",           "category": "Accessories",  "min": 9,   "max": 49},
    {"name": "Amazon Prime",          "category": "Subscription", "min": 12,  "max": 14},
    {"name": "Yoga Mat",              "category": "Sports",       "min": 19,  "max": 79},
    {"name": "Coffee Maker",          "category": "Appliances",   "min": 29,  "max": 199},
]

STATUSES    = ["CREATED", "COMPLETED", "CANCELLED", "REFUNDED"]
STATUS_PROB = [0.5, 0.35, 0.10, 0.05]   # realistic distribution
CURRENCIES  = ["USD", "EUR", "GBP", "CAD", "AUD"]
CURR_PROB   = [0.70, 0.15, 0.08, 0.04, 0.03]
METHODS     = ["PayPal Balance", "Credit Card", "Debit Card", "Bank Transfer"]


def generate_order(order_num: int) -> dict:
    """Generates one realistic fake PayPal order matching real API schema."""

    product    = random.choice(PRODUCTS)
    status     = random.choices(STATUSES, STATUS_PROB)[0]
    currency   = random.choices(CURRENCIES, CURR_PROB)[0]
    amount     = round(random.uniform(product["min"], product["max"]), 2)
    quantity   = random.randint(1, 3)
    created_at = fake.date_time_between(
        start_date="-1y",
        end_date="now",
        tzinfo=timezone.utc
    )

    return {
        # Match real PayPal Orders API schema
        "id":          f"FAKE{order_num:08d}{random.randint(1000,9999)}",
        "status":      status,
        "intent":      "CAPTURE",
        "create_time": created_at.isoformat(),
        "update_time": (created_at + timedelta(minutes=random.randint(1, 60))).isoformat(),
        "purchase_units": [
            {
                "description": product["name"],
                "category":    product["category"],
                "amount": {
                    "currency_code": currency,
                    "value":         str(round(amount * quantity, 2)),
                    "breakdown": {
                        "item_total": {
                            "currency_code": currency,
                            "value":         str(amount)
                        }
                    }
                },
                "quantity": quantity,
                "payment_method": random.choice(METHODS)
            }
        ],
        "payer": {
            "name": {
                "given_name": fake.first_name(),
                "surname":    fake.last_name()
            },
            "email_address": fake.email(),
            "payer_id":      fake.uuid4()[:13].upper(),
            "address": {
                "country_code": fake.country_code()
            }
        }
    }


def generate_dataset(num_records: int = 100000) -> list:
    """Generates a full dataset of fake orders."""
    print(f"[GENERATE] Creating {num_records:,} fake orders...")

    orders = []
    for i in range(1, num_records + 1):
        orders.append(generate_order(i))

        # Progress every 10k
        if i % 10000 == 0:
            print(f"[GENERATE] {i:,}/{num_records:,} orders generated...")

    print(f"[GENERATE] ✅ Done! {len(orders):,} orders generated.")
    return orders


def split_and_upload(orders: list, batch_size: int = 10000):
    """
    Splits orders into batches and uploads each to Azure Bronze.
    Batching avoids uploading one massive file.
    """
    total    = len(orders)
    batches  = [orders[i:i+batch_size] for i in range(0, total, batch_size)]

    print(f"\n[UPLOAD] Splitting into {len(batches)} batches of {batch_size:,} records each...")

    for i, batch in enumerate(batches, 1):
        filename = f"orders_batch_{i:03d}.json"
        print(f"[UPLOAD] Uploading batch {i}/{len(batches)}: {filename}")
        upload_json_to_bronze(batch, filename)

    print(f"\n[UPLOAD] ✅ All {total:,} records uploaded to Azure Bronze layer!")


if __name__ == "__main__":
    # Step 1 - Generate
    orders = generate_dataset(num_records=100000)

    # Step 2 - Save locally (optional backup)
    os.makedirs("data", exist_ok=True)
    with open("data/generated_orders.json", "w") as f:
        json.dump(orders[:100], f, indent=2)  # save first 100 as sample
    print("\n📁 Sample saved to: data/generated_orders.json (first 100 records)")

    # Step 3 - Upload to Azure
    split_and_upload(orders)
