import unittest
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing.utils import ReusedPySparkTestCase
from unittest.mock import MagicMock, patch
from azure_table_operations import AzureTableOperations

class TestAzureTableOperations(ReusedPySparkTestCase):

    def setUp(self):
        super().setUp()  # Set up the Spark context for testing
        self.credentials = MagicMock()  # Mock credentials
        self.account_name = "dummy_account"
        self.table_operations = AzureTableOperations(self.account_name, self.credentials)

    def test_spark_session_initialization(self):
        self.assertIsInstance(self.table_operations.spark, SparkSession)

    def test_load_table_without_filters(self):
        with patch.object(self.table_operations.spark.read, 'csv', return_value=self.spark.createDataFrame([], 'dummy_schema')) as mock_csv:
            df = self.table_operations.load_table("dummy_file_path")
            mock_csv.assert_called_once_with("abfss://dummy_file_path", header=True, inferSchema=True)
            self.assertIsInstance(df, DataFrame)

    def test_apply_filters(self):
        df = self.spark.createDataFrame(
            [(1, "John"), (2, "Jane"), (3, "Doe")],
            ["id", "name"]
        )
        filters = [{"column": "id", "operator": ">", "value": 1}]
        filtered_df = self.table_operations.apply_filters(df, filters)
        self.assertEqual(filtered_df.count(), 2)

    def tearDown(self):
        super().tearDown()  # Stop the Spark context

if __name__ == '__main__':
    unittest.main()
