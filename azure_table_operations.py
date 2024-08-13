import os
from pyspark.sql import SparkSession, DataFrame
from typing import Optional

class DatabricksSparkConnector:
    def __init__(self, tenant_id: str = "", client_id: str = "", client_secret: str = "", storage_account_name: str = ""):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.storage_account_name = storage_account_name
        self.is_running_on_databricks = 'DATABRICKS_RUNTIME_VERSION' in os.environ
        self.spark = self._initialize_spark()

    def _initialize_spark(self) -> SparkSession:
        if self.is_running_on_databricks:
            print("Running on Databricks, using the existing Spark session.")
            spark = SparkSession.builder.appName("DatabricksSparkConnector").getOrCreate()
        else:
            print("Running locally, using Databricks Connect or another local Spark setup.")
            spark = SparkSession.builder \
                .appName("DatabricksSparkConnector") \
                .config("spark.databricks.service.principal.client.id", self.client_id) \
                .config("spark.databricks.service.principal.client.secret", self.client_secret) \
                .config("spark.databricks.service.principal.tenant.id", self.tenant_id) \
                .config("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(self.storage_account_name), "OAuth") \
                .config("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(self.storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
                .config("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(self.storage_account_name), self.client_id) \
                .config("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(self.storage_account_name), self.client_secret) \
                .config("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(self.storage_account_name), f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token") \
                .getOrCreate()
        return spark

    def load_table(self, path: str, format: str = "delta", options: Optional[dict] = None) -> DataFrame:
        """Loads a table from the specified path."""
        if options is None:
            options = {}
        print(f"Loading data from {path} with format {format}")
        return self.spark.read.format(format).options(**options).load(path)

    def filter_data(self, df: DataFrame, filter_query: str) -> DataFrame:
        """Filters the DataFrame based on the given query."""
        print(f"Filtering data with query: {filter_query}")
        return df.filter(filter_query)

    def join_dataframes(self, df1: DataFrame, df2: DataFrame, join_condition: str, join_type: str = "inner") -> DataFrame:
        """Joins two DataFrames based on the specified join condition."""
        print(f"Joining DataFrames with condition: {join_condition} and join type: {join_type}")
        return df1.join(df2, join_condition, join_type)

    def join_with_query(self, df1: DataFrame, df2: DataFrame, query: str) -> DataFrame:
        """Joins two DataFrames using a SQL query."""
        df1.createOrReplaceTempView("df1")
        df2.createOrReplaceTempView("df2")
        print(f"Joining DataFrames with SQL query: {query}")
        return self.spark.sql(query)

    def stop(self):
        """Stops the Spark session."""
        print("Stopping the Spark session.")
        self.spark.stop()

# Example usage:
if __name__ == "__main__":
    connector = DatabricksSparkConnector(
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret",
        storage_account_name="your-storage-account-name"
    )

    # Load a Delta table or any supported format
    df1 = connector.load_table("abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-your-data>")
    
    # Load another DataFrame
    df2 = connector.load_table("abfss://<container>@<storage-account>.dfs.core.windows.net/<another-path-to-your-data>")
    
    # Filter the first DataFrame
    filtered_df = connector.filter_data(df1, "some_column > 100")
    
    # Join the two DataFrames
    joined_df = connector.join_dataframes(filtered_df, df2, "df1.id = df2.id")
    
    # Optionally, perform a SQL join
    sql_joined_df = connector.join_with_query(filtered_df, df2, "SELECT df1.*, df2.* FROM df1 JOIN df2 ON df1.id = df2.id")
    
    # Show the result
    sql_joined_df.show()
    
    # Stop the Spark session
    connector.stop()
