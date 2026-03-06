# ingestion/paypal_auth.py
# Handles PayPal OAuth2 authentication - generates Bearer token

import requests
import base64
from config.settings import PAYPAL_CLIENT_ID, PAYPAL_SECRET, PAYPAL_BASE_URL

def get_access_token() -> str:
    """
    Authenticates with PayPal API using OAuth2 Client Credentials flow.
    Returns a Bearer token valid for ~9 hours.
    """
    url = f"{PAYPAL_BASE_URL}/v1/oauth2/token"

    # Encode credentials as Base64 (required by PayPal)
    credentials = f"{PAYPAL_CLIENT_ID}:{PAYPAL_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()

    token = response.json()["access_token"]
    print("[AUTH] Access token obtained successfully.")
    return token


if __name__ == "__main__":
    # Quick test - run: python -m ingestion.paypal_auth
    token = get_access_token()
    print(f"Token (first 20 chars): {token[:20]}...")
