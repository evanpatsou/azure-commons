import logging
from pyspark.sql import SparkSession
from azure_credentials import AzureCredentials

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableOperations:
    """Handles loading, joining, querying tables, and running SQL queries with abfss:// using PySpark."""
    
    def __init__(self, account_name: str, credentials: AzureCredentials):
        try:
            self.spark = SparkSession.builder \
                .appName("AzureTableOperations") \
                .config("fs.azure.account.auth.type", "OAuth") \
                .config("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
                .config("fs.azure.account.oauth2.client.id", credentials.credential._client_id) \
                .config("fs.azure.account.oauth2.client.secret", credentials.credential._client_secret) \
                .config("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{credentials.credential._tenant_id}/oauth2/token") \
                .getOrCreate()
            logger.info("Spark session initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Spark session: {e}")
            raise

    def load_table(self, file_path: str, filters: list = None):
        """Loads a table from Azure Data Lake Storage Gen2 using abfss:// with optional filters for predicate pushdown."""
        try:
            abfss_url = f"abfss://{file_path}"
            logger.info(f"Loading table from {abfss_url}...")

            if filters:
                filter_query = " AND ".join([f"{f['column']} {f['operator']} {f['value']}" for f in filters if "custom_condition" not in f])
                df = self.spark.read.csv(abfss_url, header=True, inferSchema=True).filter(filter_query)
            else:
                df = self.spark.read.csv(abfss_url, header=True, inferSchema=True)

            return df
        except Exception as e:
            logger.error(f"Error loading table: {e}")
            raise

    def find_closest_date(self, df, date_column: str, target_date: str):
        """Finds the row with the closest date to the target date."""
        try:
            df = df.withColumn(date_column, df[date_column].cast("timestamp"))
            df.createOrReplaceTempView("table")
            query = f"""
            SELECT * FROM table
            ORDER BY ABS(DATEDIFF({date_column}, '{target_date}')) LIMIT 1
            """
            return self.spark.sql(query)
        except Exception as e:
            logger.error(f"Error finding closest date: {e}")
            raise

    def apply_filters(self, df, filters: list):
        """
        Applies flexible filters to the DataFrame.
        
        Args:
            df: The DataFrame to filter.
            filters: A list of filter conditions, each represented as a dictionary.
        
        Returns:
            The filtered DataFrame.
        """
        try:
            for filter_condition in filters:
                if "custom_condition" in filter_condition:
                    df = df.filter(filter_condition["custom_condition"])
                else:
                    column = filter_condition["column"]
                    operator = filter_condition["operator"]
                    value = filter_condition["value"]
                    
                    if operator == "==":
                        df = df.filter(df[column] == value)
                    elif operator == "!=":
                        df = df.filter(df[column] != value)
                    elif operator == "<":
                        df = df.filter(df[column] < value)
                    elif operator == "<=":
                        df = df.filter(df[column] <= value)
                    elif operator == ">":
                        df = df.filter(df[column] > value)
                    elif operator == ">=":
                        df = df.filter(df[column] >= value)
                    else:
                        raise ValueError(f"Unsupported operator: {operator}")

            df = df.persist()  # Persist the DataFrame if it will be reused
            return df
        except Exception as e:
            logger.error(f"Error applying filters: {e}")
            raise

    def join_tables(self, df1, df2, join_column: str, how: str = 'inner'):
        """Joins two DataFrames on a specified column with repartitioning and optional broadcast join."""
        try:
            logger.info(f"Joining tables on column '{join_column}' with '{how}' join...")

            # Repartition DataFrames on the join column to optimize the join
            df1 = df1.repartition(df1[join_column])
            df2 = df2.repartition(df2[join_column])

            # Use broadcast join if df2 is significantly smaller
            if df2.count() < 10000:  # Example threshold for broadcasting
                df2 = broadcast(df2)

            return df1.join(df2, on=join_column, how=how)
        except Exception as e:
            logger.error(f"Error joining tables: {e}")
            raise

    def run_query(self, query: str):
        """
        Runs an SQL query against the loaded data.
        
        Args:
            query (str): The SQL query to execute.
        
        Returns:
            pyspark.sql.DataFrame: The result of the query as a DataFrame.
        """
        try:
            logger.info(f"Running query: {query}")
            df = self.spark.sql(query)

            # Check the execution plan
            df.explain()

            return df
        except Exception as e:
            logger.error(f"Error running query: {e}")
            raise

    def save_as_parquet(self, df, output_path: str):
        """Saves the DataFrame as Parquet to the specified path."""
        try:
            df.write.parquet(output_path)
            logger.info(f"Data saved as Parquet at {output_path}")
        except Exception as e:
            logger.error(f"Error saving data as Parquet: {e}")
            raise