# config/settings.py
# Loads all environment variables - import this in every script

import os
from dotenv import load_dotenv

load_dotenv()

# --- PayPal ---
PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID")
PAYPAL_SECRET = os.getenv("PAYPAL_SECRET")
PAYPAL_BASE_URL = os.getenv("PAYPAL_BASE_URL", "https://api-m.sandbox.paypal.com")

# --- Azure Data Lake ---
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "paypal-bronze")

# --- Azure SQL ---
AZURE_SQL_SERVER   = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_DB       = os.getenv("AZURE_SQL_DB")
AZURE_SQL_USER     = os.getenv("AZURE_SQL_USER")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
