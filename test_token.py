import os
import dlt
import requests
from pathlib import Path
import re
import logging
from typing import Dict, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
GOOGLE_CREDENTIALS_PATH = Path(__file__).parent / "inas-sandbox-project-cf42fa268c10.json"
SECRETS_PATH = Path(".dlt/secrets.toml")
FORTNOX_API_BASE_URL = "https://api.fortnox.se"
FORTNOX_TOKEN_URL = "https://apps.fortnox.se/oauth-v1/token"

# Set Google credentials
if GOOGLE_CREDENTIALS_PATH.exists():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(GOOGLE_CREDENTIALS_PATH)
else:
    logger.warning(f"Google credentials not found at {GOOGLE_CREDENTIALS_PATH}")

from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


class FortnoxTokenManager:
    """Manages Fortnox OAuth token refresh and storage."""
    
    def __init__(self, secrets_path: Path = SECRETS_PATH):
        self.secrets_path = secrets_path
    
    def save_refresh_token(self, new_token: str) -> bool:
        """
        Save new refresh token to secrets.toml
        
        Args:
            new_token: The new refresh token to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if not self.secrets_path.exists():
            logger.error(f"secrets.toml not found at {self.secrets_path.absolute()}")
            return False
        
        try:
            content = self.secrets_path.read_text(encoding='utf-8')
            
            # Replace old token with new one
            new_content = re.sub(
                r'(fortnox_refresh_token\s*=\s*)["\']([^"\']*)["\']',
                f'fortnox_refresh_token = "{new_token}"',
                content
            )
            
            # Verify replacement happened
            if new_content == content:
                logger.error("Could not find fortnox_refresh_token in secrets.toml")
                return False
            
            self.secrets_path.write_text(new_content, encoding='utf-8')
            logger.info("✅ Refresh token saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save refresh token: {e}")
            return False
    
    def refresh_access_token(
        self, 
        client_id: str, 
        client_secret: str, 
        refresh_token: str
    ) -> Dict[str, str]:
        """
        Exchange refresh token for a new access token.
        
        Args:
            client_id: Fortnox client ID
            client_secret: Fortnox client secret
            refresh_token: Current refresh token
            
        Returns:
            dict: Contains 'access_token' and 'refresh_token'
            
        Raises:
            Exception: If token refresh fails
        """
        logger.info("🔄 Requesting new access token from Fortnox...")
        
        try:
            response = requests.post(
                FORTNOX_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=30
            )
            
            if response.status_code != 200:
                error_msg = f"Token refresh failed: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            token_data = response.json()
            access_token = token_data.get("access_token")
            new_refresh_token = token_data.get("refresh_token")
            
            if not access_token:
                raise Exception("No access_token in response")
            
            logger.info(f"✅ New access token received (expires in {token_data.get('expires_in', 'N/A')}s)")
            
            # Handle refresh token rotation
            if new_refresh_token and new_refresh_token != refresh_token:
                logger.info("🔄 Refresh token rotated, saving new token...")
                if self.save_refresh_token(new_refresh_token):
                    logger.info("✅ New refresh token saved")
                else:
                    logger.warning("⚠️ Failed to save new refresh token - please update manually")
            
            return {
                "access_token": access_token,
                "refresh_token": new_refresh_token or refresh_token
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during token refresh: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during token refresh: {e}")
            raise


@dlt.source
def fortnox_rest_api_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
):
    """
    Fetch data from Fortnox REST API endpoints.
    
    This source fetches invoices and customers from Fortnox API.
    It automatically handles token refresh and rotation.
    
    Args:
        client_id: Fortnox OAuth client ID
        client_secret: Fortnox OAuth client secret
        refresh_token: Fortnox OAuth refresh token
        
    Yields:
        DLT resources containing Fortnox data
    """
    logger.info("🚀 Starting Fortnox REST API Source")
    
    # Initialize token manager
    token_manager = FortnoxTokenManager()
    
    # Get fresh access token
    tokens = token_manager.refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens["access_token"]
    
    # Configure REST API client
    config: RESTAPIConfig = {
        "client": {
            "base_url": FORTNOX_API_BASE_URL,
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
            "headers": {
                "Accept": "application/json",
            },
        },
        "resource_defaults": {
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "invoices",
                "primary_key": "DocumentNumber",
                "endpoint": {
                    "path": "/3/invoices",
                    "method": "GET",
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
                        "offset": 0,
                        "offset_param": "offset",
                        "limit_param": "limit",
                    },
                    "data_selector": "Invoices",
                },
            },
            {
                "name": "customers",
                "primary_key": "CustomerNumber",
                "endpoint": {
                    "path": "/3/customers",
                    "method": "GET",
                    "paginator": {
                        "type": "offset",
                        "limit": 100,
                        "offset": 0,
                        "offset_param": "offset",
                        "limit_param": "limit",
                    },
                    "data_selector": "Customers",
                },
            },
        ],
    }

    logger.info("📦 Configured resources: invoices, customers")
    yield from rest_api_resources(config)


def run_pipeline():
    """Run the Fortnox to BigQuery pipeline."""
    logger.info("="*60)
    logger.info("Starting Fortnox to BigQuery Pipeline")
    logger.info("="*60)
    
    try:
        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name="fortnox_to_bigquery",
            destination="bigquery",
            dataset_name="fortnox_data",
            progress="log",
        )
        
        # Run pipeline
        load_info = pipeline.run(
            fortnox_rest_api_source(
                client_id=dlt.secrets["fortnox_client_id"],
                client_secret=dlt.secrets["fortnox_client_secret"],
                refresh_token=dlt.secrets["fortnox_refresh_token"]
            )
        )
        
        logger.info("="*60)
        logger.info("Pipeline Results:")
        logger.info(str(load_info))
        logger.info("="*60)
        logger.info("✅ Pipeline completed successfully!")
        
        return load_info
        
    except Exception as e:
        logger.error("="*60)
        logger.error("❌ Pipeline failed!")
        logger.error(f"Error: {e}")
        logger.error("="*60)
        raise


if __name__ == "__main__":
    run_pipeline()