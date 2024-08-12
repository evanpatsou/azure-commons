import logging
import pandas as pd
from typing import Iterator, Optional, Dict
from .data_processor_strategy import DataProcessorStrategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChunkedDataProcessor(DataProcessorStrategy):
    """Concrete implementation of DataProcessorStrategy for processing data in chunks."""

    def process(self, chunks: Iterator[pd.DataFrame], filters: Optional[Dict[str, str]] = None,
                date_column: Optional[str] = None, target_date: Optional[str] = None) -> pd.DataFrame:
        """Processes data chunks by applying filters and finding the closest date.

        Args:
            chunks (Iterator[pd.DataFrame]): An iterator over data chunks.
            filters (Optional[Dict[str, str]]): A dictionary of filters to apply to the data.
            date_column (Optional[str]): The name of the date column for finding the closest date.
            target_date (Optional[str]): The target date to find the closest date to.

        Returns:
            pd.DataFrame: The processed DataFrame.

        Raises:
            Exception: If there is an error processing the data.
        """
        results = []
        try:
            for chunk in chunks:
                if filters:
                    chunk = self.apply_filters(chunk, filters)
                if date_column and target_date:
                    closest_row = self.find_closest_date(chunk, date_column, target_date)
                    results.append(closest_row)
                else:
                    results.append(chunk)
            logger.info("Processing of data chunks completed successfully.")
            return pd.concat(results, ignore_index=True)
        except Exception as e:
            logger.error(f"Error processing data chunks: {e}")
            raise

    def apply_filters(self, df: pd.DataFrame, filters: Dict[str, str]) -> pd.DataFrame:
        """Applies filters to the DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to filter.
            filters (Dict[str, str]): The filters to apply.

        Returns:
            pd.DataFrame: The filtered DataFrame.

        Raises:
            Exception: If there is an error applying the filters.
        """
        try:
            query_string = ' & '.join([f'{col} == {repr(val)}' for col, val in filters.items()])
            return df.query(query_string)
        except Exception as e:
            logger.error(f"Error applying filters: {e}")
            raise

    def find_closest_date(self, df: pd.DataFrame, date_column: str, target_date: str) -> pd.Series:
        """Finds the row with the closest date to the target date.

        Args:
            df (pd.DataFrame): The DataFrame containing the date column.
            date_column (str): The name of the date column.
            target_date (str): The target date to find the closest date to.

        Returns:
            pd.Series: The row with the closest date.

        Raises:
            Exception: If there is an error finding the closest date.
        """
        try:
            df[date_column] = pd.to_datetime(df[date_column])
            target_date = pd.to_datetime(target_date)
            df['date_diff'] = abs(df[date_column] - target_date)
            closest_row = df.loc[df['date_diff'].idxmin()]
            df.drop(columns=['date_diff'], inplace=True)
            return closest_row
        except Exception as e:
            logger.error(f"Error finding closest date: {e}")
            raise

    def join_tables(self, df1: pd.DataFrame, df2: pd.DataFrame, join_column: str, how: str = 'inner') -> pd.DataFrame:
        """Joins two DataFrames on a specified column.

        Args:
            df1 (pd.DataFrame): The first DataFrame.
            df2 (pd.DataFrame): The second DataFrame.
            join_column (str): The column name to join on.
            how (str): The type of join to perform.

        Returns:
            pd.DataFrame: The joined DataFrame.

        Raises:
            Exception: If there is an error joining the tables.
        """
        try:
            logger.info(f"Joining tables on column '{join_column}' with '{how}' join...")
            return pd.merge(df1, df2, on=join_column, how=how)
        except Exception as e:
            logger.error(f"Error joining tables: {e}")
            raise
