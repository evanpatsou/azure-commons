import logging
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max, abs as spark_abs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom Exceptions
class SparkInitializationError(Exception):
    pass

class DataLoadError(Exception):
    pass

class DataSaveError(Exception):
    pass

class ConfigurationError(Exception):
    pass

class AzureTableOperations:
    """Handles reading from and saving to abfss, reading and saving to catalog, joining tables, running SQL queries, 
    and performing date-related operations using PySpark."""
    
    def __init__(self, enable_hive_support: bool = False, initial_spark_config: dict = None):
        """
        Initializes the AzureTableOperations with optional Hive support and initial Spark configurations.
        The Spark session is initialized during this process.
        """
        self.spark = None
        self.enable_hive_support = enable_hive_support
        self.spark_config = initial_spark_config if initial_spark_config else {}
        self.initialize_spark()

    def set_spark_config(self, config: dict):
        """
        Sets or updates Spark configurations.

        Args:
            config (dict): A dictionary of Spark configurations to set or update.
        """
        self.spark_config.update(config)
        logger.info("Updated Spark configuration.")
        for key, value in config.items():
            logger.info(f"{key} = {value}")

    def load_spark_config_from_file(self, file_path: str):
        """
        Loads Spark configurations from a JSON file.
        
        Args:
            file_path (str): The path to the JSON file containing Spark configurations.
        """
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
                self.set_spark_config(config)
                logger.info(f"Loaded Spark configuration from {file_path}")
        except Exception as e:
            logger.error(f"Error loading Spark configuration from file: {e}")
            raise ConfigurationError(f"Failed to load configuration from {file_path}: {str(e)}")

    def validate_config(self):
        """
        Validates the Spark configuration to ensure that all necessary parameters are set.
        """
        required_keys = ["fs.azure.account.auth.type", "fs.azure.account.oauth.provider.type", "fs.azure.account.oauth2.client.id", 
                         "fs.azure.account.oauth2.client.secret", "fs.azure.account.oauth2.client.endpoint"]
        missing_keys = [key for key in required_keys if key not in self.spark_config]
        
        if missing_keys:
            raise ConfigurationError(f"Missing required Spark configurations: {', '.join(missing_keys)}")
        logger.info("All required configurations are set.")

    def initialize_spark(self):
        """
        Initializes the Spark session with the current configuration.
        """
        try:
            if self.spark is not None:
                logger.info("Spark session already initialized.")
                return

            self.validate_config()

            builder = SparkSession.builder.appName("AzureTableOperations")
            
            if self.enable_hive_support:
                builder = builder.enableHiveSupport()
                
            for key, value in self.spark_config.items():
                builder = builder.config(key, value)
                    
            self.spark = builder.getOrCreate()
            logger.info("Spark session initialized successfully with the following configurations:")
            for key, value in self.spark.sparkContext.getConf().getAll():
                logger.info(f"{key} = {value}")
        except Exception as e:
            logger.error(f"Error initializing Spark session: {e}")
            raise SparkInitializationError(f"Failed to initialize Spark session: {str(e)}")

    def reinitialize_spark(self):
        """
        Reinitializes the Spark session with the current configuration. This method should be called if 
        configurations are changed after the initial Spark session has been created.
        """
        if self.spark is not None:
            logger.info("Stopping the existing Spark session.")
            self.spark.stop()
            self.spark = None
        self.initialize_spark()

    def load_data(self, path: str, format: str = "parquet") -> DataFrame:
        """
        Loads data from a specified path, which could be either abfss or local file system.
        
        Args:
            path (str): The path to the data (can be abfss:// or local file system path).
            format (str): The format of the data (e.g., 'parquet', 'csv'). Default is 'parquet'.
        
        Returns:
            DataFrame: The loaded DataFrame.
        """
        try:
            if self.spark is None:
                self.initialize_spark()
                
            logger.info(f"Loading data from {path}...")
            df = self.spark.read.format(format).load(path)
            logger.info("Data loaded successfully.")
            return df
        except Exception as e:
            logger.error(f"Error loading data from {path}: {e}")
            raise DataLoadError(f"Failed to load data from {path}: {str(e)}")

    def save_data(self, df: DataFrame, path: str, format: str = "parquet", mode: str = "overwrite"):
        """
        Saves a DataFrame to a specified path, which could be either abfss or local file system.
        
        Args:
            df (DataFrame): The DataFrame to save.
            path (str): The destination path (can be abfss:// or local file system path).
            format (str): The format to save the data (e.g., 'parquet', 'csv'). Default is 'parquet'.
            mode (str): The write mode (e.g., 'overwrite', 'append'). Default is 'overwrite'.
        """
        try:
           
            logger.info(f"Saving DataFrame to {path} as {format} with mode {mode}...")
            df.write.format(format).mode(mode).save(path)
            logger.info("DataFrame saved successfully.")
        except Exception as e:
            logger.error(f"Error saving DataFrame to {path}: {e}")
            raise DataSaveError(f"Failed to save data to {path}: {str(e)}")

    def read_from_catalog(self, table_name: str, catalog: str = None) -> DataFrame:
        """
        Reads a table from the local catalog (e.g., Hive) into a DataFrame.
        
        Args:
            table_name (str): The name of the table to read.
            catalog (str): The catalog or database name (optional).
        
        Returns:
            DataFrame: The DataFrame containing the table data.
        """
        try:
            if self.spark is None:
                self.initialize_spark()
                
            full_table_name = f"{catalog}.{table_name}" if catalog else table_name
            logger.info(f"Reading table from catalog: {full_table_name}")
            df = self.spark.table(full_table_name)
            logger.info(f"Table {full_table_name} read successfully.")
            return df
        except Exception as e:
            logger.error(f"Error reading table from catalog: {e}")
            raise DataLoadError(f"Failed to read table {full_table_name}: {str(e)}")

    def save_to_catalog(self, df: DataFrame, table_name: str, mode: str = "overwrite", catalog: str = None):
        """
        Stores a DataFrame in an external catalog (e.g., Hive).
        
        Args:
            df (DataFrame): The DataFrame to store.
            table_name (str): The name of the table to store the data in.
            mode (str): The write mode (e.g., "overwrite", "append"). Default is "overwrite".
            catalog (str): The catalog or database name (optional).
        """
        try:
            if self.spark is None:
                self.initialize_spark()
                
            full_table_name = f"{catalog}.{table_name}" if catalog else table_name
            logger.info(f"Storing DataFrame to catalog: {full_table_name} with mode: {mode}")
            df.write.mode(mode).saveAsTable(full_table_name)
            logger.info(f"DataFrame stored to catalog: {full_table_name} successfully.")
        except Exception as e:
            logger.error(f"Error storing DataFrame to catalog: {e}")
            raise DataSaveError(f"Failed to store DataFrame to catalog {full_table_name}: {str(e)}")

    def join_tables(self, df1: DataFrame, df2: DataFrame, join_column: str, how: str = 'inner') -> DataFrame:
        """Joins two DataFrames on a specified column."""
        try:
            if self.spark is None:
                self.initialize_spark()
                
            logger.info(f"Joining tables on column '{join_column}' with '{how}' join...")
            result_df = df1.join(df2, on=join_column, how=how)
            logger.info("Tables joined successfully.")
            return result_df
        except Exception as e:
            logger.error(f"Error joining tables: {e}")
            raise DataLoadError(f"Failed to join tables on column {join_column}: {str(e)}")

    def run_query(self, query: str) -> DataFrame:
        """
        Runs an SQL query against the loaded data.
        
        Args:
            query (str): The SQL query to execute.
        
        Returns:
            DataFrame: The result of the query as a DataFrame.
        """
        try:
            if self.spark is None:
                self.initialize_spark()
                
            logger.info(f"Running query: {query}")
            df = self.spark.sql(query)
            logger.info("Query executed successfully.")
            return df
        except Exception as e:
            logger.error(f"Error running query: {e}")
            raise DataLoadError(f"Failed to execute query: {str(e)}")

    def find_closest_date(self, df: DataFrame, date_column: str, target_date: str) -> DataFrame:
        """
        Finds the row with the closest date to the target date.
        
        Args:
            df (DataFrame): The DataFrame containing the data.
            date_column (str): The name of the date column.
            target_date (str): The target date to find the closest match.
        
        Returns:
            DataFrame: The DataFrame containing the row with the closest date.
        """
        try:
            logger.info(f"Finding closest date to '{target_date}' in column '{date_column}'...")
            df = df.withColumn("date_diff", spark_abs(col(date_column).cast("timestamp") - col("timestamp").cast("timestamp")))
            closest_date_df = df.orderBy("date_diff").limit(1)
            logger.info("Closest date found successfully.")
            return closest_date_df
        except Exception as e:
            logger.error(f"Error finding closest date: {e}")
            raise DataLoadError(f"Failed to find closest date for target {target_date}: {str(e)}")

    def get_latest_date(self, df: DataFrame, id_column: str, date_column: str, num_partitions: int = None) -> DataFrame:
        """
        Retrieves the latest date for each ID, optimized with partitioning if required.
        
        Args:
            df (DataFrame): The DataFrame containing the data.
            id_column (str): The name of the ID column.
            date_column (str): The name of the date column.
            num_partitions (int): The number of partitions to use for the operation (optional).
        
        Returns:
            DataFrame: The DataFrame containing the latest date for each ID.
        """
        try:
            logger.info(f"Getting the latest date for each ID in column '{date_column}'...")
            
            if num_partitions:
                df = df.repartition(num_partitions, id_column)
            
            latest_date_df = df.groupBy(id_column).agg(spark_max(col(date_column)).alias("latest_date"))
            logger.info("Latest date retrieved successfully.")
            return latest_date_df
        except Exception as e:
            logger.error(f"Error getting the latest date: {e}")
            raise DataLoadError(f"Failed to get latest date for column {date_column}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Initialize the class with initial Spark configurations
    operations = AzureTableOperations(enable_hive_support=True)

    # Set additional Spark configurations
    operations.set_spark_config({
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": "<client_id>",
        "fs.azure.account.oauth2.client.secret": "<client_secret>",
        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant_id>/oauth2/token"
    })

    # Load data from abfss
    df_abfss = operations.load_data("abfss://<container>@<storage_account>.dfs.core.windows.net/<path_to_file>.parquet")
    df_abfss.show()

    # Save the DataFrame to abfss
    operations.save_data(df_abfss, "abfss://<container>@<storage_account>.dfs.core.windows.net/<path_to_save>/")

    # Read from a local catalog (e.g., Hive table)
    df_catalog = operations.read_from_catalog("person_table", catalog="default")
    df_catalog.show()

    # Get the latest date for each ID
    latest_df = operations.get_latest_date(df_catalog, id_column="id", date_column="date")
    latest_df.show()

    # Save the result back to the catalog
    operations.save_to_catalog(latest_df, table_name="latest_person_dates", catalog="default")
