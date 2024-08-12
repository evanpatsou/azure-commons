from azure.storage.blob import BlobServiceClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureBlobClient:
    """Handles operations with Azure Blob Storage."""
    
    def __init__(self, account_name: str, credential: ClientSecretCredential):
        try:
            self.blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=credential
            )
            logger.info("Azure Blob Service Client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Azure Blob Service Client: {e}")
            raise

    def download_blob(self, container_name: str, blob_name: str, download_path: str):
        """Downloads a file from Azure Blob Storage."""
        try:
            logger.info(f"Downloading blob '{blob_name}' from container '{container_name}' to '{download_path}'...")
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            with open(download_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            logger.info(f"Blob '{blob_name}' downloaded successfully.")
        except Exception as e:
            logger.error(f"Error downloading blob: {e}")
            raise

    def upload_blob(self, container_name: str, file_path: str, blob_name: str):
        """Uploads a file to Azure Blob Storage."""
        try:
            logger.info(f"Uploading file '{file_path}' as blob '{blob_name}' to container '{container_name}'...")
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logger.info(f"File '{file_path}' uploaded as blob '{blob_name}' successfully.")
        except Exception as e:
            logger.error(f"Error uploading blob: {e}")
            raise
