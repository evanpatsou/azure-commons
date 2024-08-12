import logging
from azure.identity import ClientSecretCredential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureCredentialFactory:
    """Factory class for creating Azure credentials."""

    @staticmethod
    def create_credential(credential_type: str, tenant_id: str, client_id: str, client_secret: str) -> ClientSecretCredential:
        """Creates an Azure credential based on the provided type.

        Args:
            credential_type (str): The type of credential to create.
            tenant_id (str): The Azure Tenant ID.
            client_id (str): The Azure Client ID.
            client_secret (str): The Azure Client Secret.

        Returns:
            ClientSecretCredential: The created Azure credential.

        Raises:
            ValueError: If the credential type is not supported.
            Exception: If there is an error creating the credential.
        """
        try:
            if credential_type == 'ClientSecretCredential':
                logger.info("Creating ClientSecretCredential...")
                return ClientSecretCredential(tenant_id, client_id, client_secret)
            else:
                raise ValueError(f"Unsupported credential type: {credential_type}")
        except Exception as e:
            logger.error(f"Error creating credential: {e}")
            raise
