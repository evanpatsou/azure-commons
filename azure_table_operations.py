import os
import requests
from pyspark.sql import SparkSession
from azure.identity import ClientSecretCredential

class DatabricksSparkConnector:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, 
                 storage_account_name: str, databricks_workspace_url: str, cluster_id: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.storage_account_name = storage_account_name
        self.databricks_workspace_url = databricks_workspace_url
        self.cluster_id = cluster_id
        self.is_running_on_databricks = 'DATABRICKS_RUNTIME_VERSION' in os.environ
        self.spark = self._initialize_spark()

    def _initialize_spark(self) -> SparkSession:
        if self.is_running_on_databricks:
            print("Running on Databricks, using the existing Spark session.")
            spark = SparkSession.builder.appName("DatabricksSparkConnector").getOrCreate()
        else:
            print("Running locally, connecting to Databricks cluster.")
            # Authenticate to Azure to get a token
            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            token = credential.get_token("https://management.azure.com/.default").token
            
            # Prepare headers for Databricks REST API
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            }

            # Check if the cluster is running
            response = requests.get(f'{self.databricks_workspace_url}/api/2.0/clusters/get?cluster_id={self.cluster_id}', 
                                    headers=headers)
            cluster_info = response.json()

            if response.status_code == 200:
                print(f"Cluster status: {cluster_info['state']}")
                if cluster_info['state'] != 'RUNNING':
                    print("Starting cluster...")
                    start_response = requests.post(f'{self.databricks_workspace_url}/api/2.0/clusters/start', 
                                                   headers=headers, json={"cluster_id": self.cluster_id})
                    if start_response.status_code != 200:
                        raise Exception(f"Failed to start cluster: {start_response.text}")
                    else:
                        print("Cluster started successfully.")
                else:
                    print("Cluster is already running.")
            else:
                raise Exception(f"Failed to get cluster info: {response.text}")

            # Create a Spark session
            spark = SparkSession.builder \
                .appName("DatabricksSparkConnector") \
                .config("spark.master", f"spark://{self.cluster_id}:7077") \
                .getOrCreate()
            
        return spark

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
