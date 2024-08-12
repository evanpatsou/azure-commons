import logging
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import FileSystemClient
from azure.identity import ClientSecretCredential
from azure_data_loader.data_loader.data_loader_strategy import DataLoaderStrategy
from azure_data_loader.data_processor.data_processor_strategy import DataProcessorStrategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureDataLoader:
    """Class for loading and processing data from Azure Data Lake."""

    def __init__(self, account_name: str, container_name: str, credential: ClientSecretCredential):
        """Initializes the AzureDataLoader with account details and credentials.

        Args:
            account_name (str): The Azure Storage Account name.
            container_name (str): The Azure Data Lake container name.
            credential (ClientSecretCredential): The Azure ClientSecretCredential for authentication.

        Raises:
            Exception: If there is an error initializing the DataLakeServiceClient.
        """
        try:
            self.account_name = account_name
            self.container_name = container_name
            self.service_client = self._create_service_client(credential)
            logger.info("AzureDataLoader initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing AzureDataLoader: {e}")
            raise

    def _create_service_client(self, credential: ClientSecretCredential) -> DataLakeServiceClient:
        """Creates an Azure DataLakeServiceClient.

        Args:
            credential (ClientSecretCredential): The Azure ClientSecretCredential for authentication.

        Returns:
            DataLakeServiceClient: The created DataLakeServiceClient.

        Raises:
            Exception: If there is an error creating the service client.
        """
        try:
            logger.info("Creating DataLakeServiceClient...")
            return DataLakeServiceClient(account_url=f"https://{self.account_name}.dfs.core.windows.net", credential=credential)
        except Exception as e:
            logger.error(f"Error creating DataLakeServiceClient: {e}")
            raise

    def load_table(self, file_path: str, loader_strategy: DataLoaderStrategy, chunksize: int = 1000000):
        """Loads a table from Azure Data Lake using the specified strategy.

        Args:
            file_path (str): The path to the file in the Azure Data Lake.
            loader_strategy (DataLoaderStrategy): The strategy to use for loading the data.
            chunksize (int): The size of the data chunks to load.

        Returns:
            Iterator[pd.DataFrame]: An iterator over the loaded data chunks.

        Raises:
            Exception: If there is an error loading the table.
        """
        try:
            file_system_client = self.service_client.get_file_system_client(file_system=self.container_name)
            logger.info(f"Loading table from {file_path}...")
            return loader_strategy.load_data(file_system_client, file_path, chunksize)
        except Exception as e:
            logger.error(f"Error loading table: {e}")
            raise

    def process_chunks(self, chunks, processor_strategy: DataProcessorStrategy, filters=None, date_column=None, target_date=None):
        """Processes data chunks using the specified strategy.

        Args:
            chunks: An iterator over data chunks.
            processor_strategy (DataProcessorStrategy): The strategy to use for processing the data.
            filters (dict): A dictionary of filters to apply to the data.
            date_column (str): The name of the date column to use for finding the closest date.
            target_date (str): The target date to find the closest date to.

        Returns:
            pd.DataFrame: The processed DataFrame.

        Raises:
            Exception: If there is an error processing the chunks.
        """
        try:
            logger.info("Processing data chunks...")
            return processor_strategy.process(chunks, filters, date_column, target_date)
        except Exception as e:
            logger.error(f"Error processing chunks: {e}")
            raise

    def join_tables(self, df1, df2, processor_strategy: DataProcessorStrategy, join_column: str, how: str = 'inner'):
        """Joins two tables using the specified strategy.

        Args:
            df1: The first DataFrame.
            df2: The second DataFrame.
            processor_strategy (DataProcessorStrategy): The strategy to use for joining the tables.
            join_column (str): The column to join the tables on.
            how (str): The type of join to perform.

        Returns:
            pd.DataFrame: The joined DataFrame.

        Raises:
            Exception: If there is an error joining the tables.
        """
        try:
            logger.info(f"Joining tables on column '{join_column}'...")
            return processor_strategy.join_tables(df1, df2, join_column, how)
        except Exception as e:
            logger.error(f"Error joining tables: {e}")
            raise
