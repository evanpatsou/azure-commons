import unittest
from unittest.mock import patch
from azure_credentials import AzureCredentials
from azure.identity import ClientSecretCredential

class TestAzureCredentials(unittest.TestCase):

    @patch('azure_credentials.ClientSecretCredential')
    def test_credentials_initialization(self, MockCredential):
        mock_credential = MockCredential.return_value
        credentials = AzureCredentials('tenant_id', 'client_id', 'client_secret')
        self.assertIsInstance(credentials.credential, ClientSecretCredential)
        MockCredential.assert_called_once_with('tenant_id', 'client_id', 'client_secret')

    @patch('azure_credentials.ClientSecretCredential')
    def test_credentials_initialization_failure(self, MockCredential):
        MockCredential.side_effect = Exception("Initialization error")
        with self.assertRaises(Exception) as context:
            AzureCredentials('tenant_id', 'client_id', 'client_secret')
        self.assertTrue('Initialization error' in str(context.exception))

if __name__ == '__main__':
    unittest.main()
