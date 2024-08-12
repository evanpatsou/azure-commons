import logging
from abc import ABC, abstractmethod
from azure.storage.filedatalake import FileSystemClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoaderStrategy(ABC):
    """Abstract base class for data loading strategies."""

    @abstractmethod
    def load_data(self, file_system_client: FileSystemClient, file_path: str, chunksize: int):
        """Loads data from the specified file.

        Args:
            file_system_client (FileSystemClient): The file system client to use for accessing the file.
            file_path (str): The path to the file in the data lake.
            chunksize (int): The size of the data chunks to load.

        Returns:
            An iterator over data chunks.

        Raises:
            Exception: If there is an error loading the data.
        """
        pass
