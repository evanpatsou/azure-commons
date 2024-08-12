from azure.identity import ClientSecretCredential
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureCredentials:
    """Handles Azure authentication and credential management."""
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        try:
            self.credential = ClientSecretCredential(tenant_id, client_id, client_secret)
            logger.info("Azure credentials initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Azure credentials: {e}")
            raise
