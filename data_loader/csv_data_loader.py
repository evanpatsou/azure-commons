import logging
import pandas as pd
from azure.storage.filedatalake import FileSystemClient
from .data_loader_strategy import DataLoaderStrategy
from typing import Iterator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVDataLoader(DataLoaderStrategy):
    """Concrete implementation of DataLoaderStrategy for loading CSV files."""

    def load_data(self, file_system_client: FileSystemClient, file_path: str, chunksize: int = 1000000) -> Iterator[pd.DataFrame]:
        """Loads CSV data from the specified file in chunks.

        Args:
            file_system_client (FileSystemClient): The file system client to use for accessing the file.
            file_path (str): The path to the CSV file in the data lake.
            chunksize (int): The size of the data chunks to load.

        Returns:
            Iterator[pd.DataFrame]: An iterator over DataFrames, each containing a chunk of data.

        Raises:
            Exception: If there is an error loading the data.
        """
        try:
            logger.info(f"Loading CSV data from {file_path} with chunksize={chunksize}...")
            file_client = file_system_client.get_file_client(file_path)
            download = file_client.download_file()
            downloaded_bytes = download.readall()

            from io import BytesIO
            return pd.read_csv(BytesIO(downloaded_bytes), chunksize=chunksize)
        except Exception as e:
            logger.error(f"Error loading CSV data: {e}")
            raise
