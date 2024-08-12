import unittest
import pandas as pd
from azure_data_loader.data_processor.chunked_data_processor import ChunkedDataProcessor

class TestChunkedDataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up the required variables for the tests."""
        self.processor = ChunkedDataProcessor()
        data = {'date_column': ['2024-08-10', '2024-08-11', '2024-08-09'], 'value': [10, 20, 30]}
        self.df = pd.DataFrame(data)

    def test_process_success(self):
        """Test successful processing of data chunks."""
        chunks = [self.df]
        processed_df = self.processor.process(chunks, date_column='date_column', target_date='2024-08-10')

        self.assertIsInstance(processed_df, pd.DataFrame)
        self.assertEqual(len(processed_df), 1)
        self.assertEqual(processed_df.iloc[0]['value'], 10)

    def test_apply_filters(self):
        """Test applying filters to a DataFrame."""
        filters = {'value': 20}
        filtered_df = self.processor.apply_filters(self.df, filters)

        self.assertEqual(len(filtered_df), 1)
        self.assertEqual(filtered_df.iloc[0]['value'], 20)

    def test_find_closest_date(self):
        """Test finding the closest date."""
        closest_row = self.processor.find_closest_date(self.df, 'date_column', '2024-08-10')

        self.assertEqual(closest_row['value'], 10)

    def test_join_tables(self):
        """Test joining two DataFrames."""
        df1 = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
        df2 = pd.DataFrame({'id': [1, 3], 'other_value': [30, 40]})

        joined_df = self.processor.join_tables(df1, df2, 'id')
        self.assertEqual(len(joined_df), 1)
        self.assertEqual(joined_df.iloc[0]['other_value'], 30)

if __name__ == '__main__':
    unittest.main()
