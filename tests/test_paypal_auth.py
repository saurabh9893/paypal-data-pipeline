# tests/test_paypal_auth.py
import pytest
from unittest.mock import patch, MagicMock

def test_get_access_token_success():
    """Test that OAuth token is retrieved correctly."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "test_token_123"}

    with patch("requests.post", return_value=mock_response):
        from ingestion.paypal_auth import get_access_token
        token = get_access_token()
        assert token == "test_token_123"

def test_get_access_token_failure():
    """Test that HTTP errors are raised properly."""
    with patch("requests.post") as mock_post:
        mock_post.return_value.raise_for_status.side_effect = Exception("401 Unauthorized")
        with pytest.raises(Exception):
            from ingestion.paypal_auth import get_access_token
            get_access_token()
