import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from azure_table_operations import TableOperations
from pyspark.sql.functions import broadcast

class TestAzureTableOperations(unittest.TestCase):

    @patch('azure_table_operations.SparkSession.builder.getOrCreate')
    def setUp(self, MockSparkSession):
        self.mock_spark = MockSparkSession.return_value
        self.credentials = MagicMock()  # Mock credentials
        self.account_name = "dummy_account"
        self.table_operations = AzureTableOperations(self.account_name, self.credentials)

    def test_spark_session_initialization(self):
        self.assertIsInstance(self.table_operations.spark, SparkSession)

    def test_load_table_with_filters(self):
        mock_df = MagicMock(spec=DataFrame)
        self.mock_spark.read.csv.return_value = mock_df
        filters = [{"column": "age", "operator": ">", "value": 30}]
        df = self.table_operations.load_table("dummy_file_path", filters=filters)
        filter_query = "age > 30"
        self.mock_spark.read.csv.assert_called_once_with("abfss://dummy_file_path", header=True, inferSchema=True)
        mock_df.filter.assert_called_once_with(filter_query)
        self.assertIs(df, mock_df)

    def test_load_table_without_filters(self):
        mock_df = MagicMock(spec=DataFrame)
        self.mock_spark.read.csv.return_value = mock_df
        df = self.table_operations.load_table("dummy_file_path")
        self.mock_spark.read.csv.assert_called_once_with("abfss://dummy_file_path", header=True, inferSchema=True)
        self.assertIs(df, mock_df)

    def test_find_closest_date(self):
        mock_df = MagicMock(spec=DataFrame)
        self.mock_spark.sql.return_value = mock_df
        result = self.table_operations.find_closest_date(mock_df, "date_column", "2024-01-01")
        self.mock_spark.sql.assert_called_once()
        self.assertIs(result, mock_df)

    def test_apply_filters(self):
        mock_df = MagicMock(spec=DataFrame)
        filters = [
            {"column": "age", "operator": ">", "value": 30},
            {"custom_condition": "age > 30 AND name == 'John'"}
        ]
        result = self.table_operations.apply_filters(mock_df, filters)
        self.assertIsInstance(result, MagicMock)
        self.assertEqual(mock_df.filter.call_count, 2)
        mock_df.persist.assert_called_once()

    def test_join_tables_with_broadcast(self):
        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)
        mock_df2.count.return_value = 5000  # Simulate a small DataFrame
        self.table_operations.join_tables(mock_df1, mock_df2, "id")
        mock_df1.repartition.assert_called_once_with(mock_df1["id"])
        mock_df2.repartition.assert_called_once_with(mock_df2["id"])
        self.mock_spark.sql.broadcast.assert_called_once_with(mock_df2)
        mock_df1.join.assert_called_once_with(broadcast(mock_df2), on="id", how="inner")

    def test_join_tables_without_broadcast(self):
        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)
        mock_df2.count.return_value = 20000  # Simulate a larger DataFrame
        self.table_operations.join_tables(mock_df1, mock_df2, "id")
        mock_df1.repartition.assert_called_once_with(mock_df1["id"])
        mock_df2.repartition.assert_called_once_with(mock_df2["id"])
        mock_df1.join.assert_called_once_with(mock_df2, on="id", how="inner")

    def test_run_query(self):
        query = "SELECT * FROM table"
        mock_df = MagicMock(spec=DataFrame)
        self.mock_spark.sql.return_value = mock_df
        result = self.table_operations.run_query(query)
        self.mock_spark.sql.assert_called_once_with(query)
        mock_df.explain.assert_called_once()
        self.assertIs(result, mock_df)

    def test_save_as_parquet(self):
        mock_df = MagicMock(spec=DataFrame)
        self.table_operations.save_as_parquet(mock_df, "output/path")
        mock_df.write.parquet.assert_called_once_with("output/path")

if __name__ == '__main__':
    unittest.main()
