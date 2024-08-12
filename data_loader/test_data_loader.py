import unittest
import pandas as pd
from azure.storage.filedatalake import FileSystemClient
from azure_data_loader.data_loader.csv_data_loader import CSVDataLoader

class TestCSVDataLoader(unittest.TestCase):
    def setUp(self):
        """Set up the required variables for the tests."""
        self.file_system_client = unittest.mock.create_autospec(FileSystemClient)
        self.file_path = "dummy_file_path.csv"
        self.loader = CSVDataLoader()

    def test_load_data_success(self):
        """Test successful data loading."""
        # Mocking the file client and download
        file_client_mock = self.file_system_client.get_file_client.return_value
        file_client_mock.download_file.return_value.readall.return_value = b"col1,col2\nval1,val2\nval3,val4"

        chunks = self.loader.load_data(self.file_system_client, self.file_path)
        df = next(chunks)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["col1"], "val1")

    def test_load_data_invalid_file_path(self):
        """Test loading data with an invalid file path."""
        with self.assertRaises(Exception):
            self.loader.load_data(self.file_system_client, None)

if __name__ == '__main__':
    unittest.main()
