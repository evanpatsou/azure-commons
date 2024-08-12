import unittest
from azure.identity import ClientSecretCredential
from azure_data_loader.credentials.credentials import CredentialFactory

class TestAzureCredentialFactory(unittest.TestCase):
    def setUp(self):
        """Set up the required variables for the tests."""
        self.tenant_id = "dummy_tenant_id"
        self.client_id = "dummy_client_id"
        self.client_secret = "dummy_client_secret"

    def test_create_client_secret_credential(self):
        """Test creating a ClientSecretCredential with valid inputs."""
        credential = AzureCredentialFactory.create_credential(
            credential_type="ClientSecretCredential",
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        self.assertIsInstance(credential, ClientSecretCredential)
    
    def test_invalid_credential_type(self):
        """Test that an invalid credential type raises a ValueError."""
        with self.assertRaises(ValueError):
            AzureCredentialFactory.create_credential(
                credential_type="InvalidCredentialType",
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )

    def test_missing_credentials(self):
        """Test that missing credentials raise an error."""
        with self.assertRaises(TypeError):
            AzureCredentialFactory.create_credential(
                credential_type="ClientSecretCredential",
                tenant_id=None,
                client_id=None,
                client_secret=None
            )

if __name__ == '__main__':
    unittest.main()
