import argparse
import logging
from azure_data_loader.credentials.credential_factory import AzureCredentialFactory
from azure_data_loader.azure_data_loader import AzureDataLoader
from azure_data_loader.data_loader.csv_data_loader import CSVDataLoader
from azure_data_loader.data_processor.chunked_data_processor import ChunkedDataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the Azure Data Loader."""
    parser = argparse.ArgumentParser(description="Azure Data Loader")
    parser.add_argument("--tenant_id", required=True, help="Azure Tenant ID")
    parser.add_argument("--client_id", required=True, help="Azure Client ID")
    parser.add_argument("--client_secret", required=True, help="Azure Client Secret")
    parser.add_argument("--account_name", required=True, help="Azure Storage Account Name")
    parser.add_argument("--container_name", required=True, help="Azure Data Lake Container Name")
    parser.add_argument("--file_path", required=True, help="Path to the file in the Azure Data Lake")
    parser.add_argument("--chunksize", type=int, default=1000000, help="Size of data chunks to process at a time")

    args = parser.parse_args()

    try:
        # Create the credential using the factory
        credential = AzureCredentialFactory.create_credential(
            credential_type='ClientSecretCredential',
            tenant_id=args.tenant_id,
            client_id=args.client_id,
            client_secret=args.client_secret
        )

        # Create the Azure Data Loader with the appropriate strategies
        loader = AzureDataLoader(
            account_name=args.account_name,
            container_name=args.container_name,
            credential=credential
        )

        # Load data using CSV loader strategy
        csv_loader = CSVDataLoader()
        chunks = loader.load_table(args.file_path, loader_strategy=csv_loader, chunksize=args.chunksize)

        # Process data using chunked data processor strategy
        chunked_processor = ChunkedDataProcessor()
        filters = {'column1': 'value1'}  # Replace with your actual filters
        closest_date_column = 'date_column'  # Replace with your actual date column
        target_date = '2024-08-10'  # Replace with your target date

        processed_data = loader.process_chunks(chunks, processor_strategy=chunked_processor, filters=filters, date_column=closest_date_column, target_date=target_date)

        # Example of joining with another dataset
        # Assuming df2 is loaded separately
        # df2 = loader.load_table('path/to/another/file.csv', loader_strategy=csv_loader, chunksize=args.chunksize)
        # joined_data = loader.join_tables(processed_data, df2, processor_strategy=chunked_processor, join_column='common_column', how='inner')

        print(processed_data.head())

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
