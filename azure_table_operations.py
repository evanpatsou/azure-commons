import os
import requests
from pyspark.sql import SparkSession
from azure.identity import ClientSecretCredential

class DatabricksSparkConnector:
    def __init__(self, databricks_host: str = None, databricks_cluster_id: str = None,
                 client_id: str = None, client_secret: str = None, tenant_id: str = None):
        """
        Initializes the DatabricksConnector class with service principal credentials.

        Args:
            databricks_host (str, optional): The Databricks workspace URL.
            databricks_cluster_id (str, optional): The ID of the Databricks cluster to connect to.
            client_id (str, optional): The client ID of the Azure AD service principal.
            client_secret (str, optional): The client secret of the Azure AD service principal.
            tenant_id (str, optional): The tenant ID of the Azure AD service principal.
        """
        self.databricks_host = databricks_host
        self.databricks_cluster_id = databricks_cluster_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

        if databricks_host and databricks_cluster_id:
            # Local environment with Databricks Connect
            self._setup_local_databricks_session()
        else:
            # On Databricks cluster
            self._setup_databricks_session()

    def _setup_local_databricks_session(self) -> None:
        """Sets up a Databricks session for local development using Databricks Connect with service principal credentials."""
        # Authenticate using the service principal credentials
        credential = ClientSecretCredential(tenant_id=self.tenant_id, client_id=self.client_id, client_secret=self.client_secret)
        
        # Get the access token for Azure AD
        token = credential.get_token("https://management.azure.com/.default").token

        # Configure Databricks Connect
        os.environ['DATABRICKS_HOST'] = self.databricks_host
        os.environ['DATABRICKS_TOKEN'] = token
        os.environ['DATABRICKS_CLUSTER_ID'] = self.databricks_cluster_id

        # Initialize DatabricksSession for Databricks Connect
        self.session = DatabricksSession.builder \
            .appName("DatabricksConnector") \
            .config("spark.databricks.service.client.enabled", "true") \
            .config("spark.databricks.service.client.username", self.client_id) \
            .config("spark.databricks.service.client.password", token) \
            .config("spark.databricks.service.tenantId", self.tenant_id) \
            .getOrCreate()
    def load_table(self, path: str, format: str = "delta", options: dict = None):
        """Loads a table from the specified path."""
        if options is None:
            options = {}
        print(f"Loading data from {path} with format {format}")
        return self.spark.read.format(format).options(**options).load(path)

    def filter_data(self, df, filter_query: str):
        """Filters the DataFrame based on the given query."""
        print(f"Filtering data with query: {filter_query}")
        return df.filter(filter_query)

    def join_dataframes(self, df1, df2, join_condition: str, join_type: str = "inner"):
        """Joins two DataFrames based on the specified join condition."""
        print(f"Joining DataFrames with condition: {join_condition} and join type: {join_type}")
        return df1.join(df2, join_condition, join_type)
    def join_with_query(self, df1, df2, query: str):
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
        storage_account_name="your-storage-account-name",
        databricks_workspace_url="https://your-databricks-instance.azuredatabricks.net",
        cluster_id="your-existing-cluster-id"
    )

    # Example: Load a table, filter, and join
    df1 = connector.load_table("abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-your-data>")
    df2 = connector.load_table("abfss://<container>@<storage-account>.dfs.core.windows.net/<another-path-to-your-data>")
    
    filtered_df = connector.filter_data(df1, "some_column > 100")
    joined_df = connector.join_dataframes(filtered_df, df2, "df1.id = df2.id")
    
    # SQL join example
    sql_joined_df = connector.join_with_query(filtered_df, df2, "SELECT df1.*, df2.* FROM df1 JOIN df2 ON df1.id = df2.id")
    
    sql_joined_df.show()
    
    connector.stop()
