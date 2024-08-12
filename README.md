# Azure Data Operations

## Overview

This project provides a Python-based solution for handling data operations in Azure, specifically using Azure Data Lake Storage Gen2 and Azure Blob Storage. The solution is designed to work efficiently with large datasets (e.g., 40 million rows) and integrates with PySpark for distributed data processing.

The key components include:
- **AzureCredentials**: Handles Azure authentication and credential management.
- **AzureBlobClient**: Manages file operations such as uploading and downloading files from Azure Blob Storage.
- **AzureTableOperations**: Performs data operations such as loading, filtering, joining, and querying tables stored in Azure Data Lake Storage Gen2 using `abfss://` paths.

## Features

- **Distributed Processing**: Leverages PySpark (3.5.0) for processing large datasets.
- **Flexible Filtering**: Supports various filter operations, including custom SQL-like conditions.
- **Azure Integration**: Direct interaction with Azure Data Lake Storage Gen2 and Blob Storage using Azure SDK.
- **Error Handling**: Robust error handling across all components to ensure smooth operations.
- **High Test Coverage**: Unit tests ensure the solution is reliable, with a goal of 95% test coverage.

## Installation

### Prerequisites

- Python 3.7 or later
- Azure account with access to Data Lake Storage Gen2 and Blob Storage
- PySpark 3.5.0
- Necessary Python libraries

### Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/azure-data-operations.git
   cd azure-data-operations
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PySpark:**
   Ensure that PySpark 3.5.0 is installed and properly configured in your environment.

## Usage

### Command-Line Arguments

The `main.py` script accepts several command-line arguments for flexibility:

- `--tenant_id`: Azure Tenant ID (required)
- `--client_id`: Azure Client ID (required)
- `--client_secret`: Azure Client Secret (required)
- `--account_name`: Azure Storage Account Name (required)
- `--container_name`: Azure Blob Container or Data Lake Container Name (required)
- `--file_path`: Path to the file in Azure Data Lake Storage Gen2 (`abfss://`)
- `--download_path`: Local path to download a file from Azure Blob Storage
- `--upload_path`: Path to upload a file to Azure Data Lake Storage Gen2 (`abfss://`)
- `--local_file`: Local file path for upload
- `--date_column`: Column name to find the closest date
- `--target_date`: Target date to find the closest match
- `--filters`: JSON string of filters to apply to the data (e.g., `--filters '{"column": "age", "operator": ">", "value": 30}'`)
- `--join_file`: File path for the table to join with
- `--join_column`: Column name to join on

### Example Commands

1. **Load and process data from Azure Data Lake Storage Gen2:**
   ```bash
   python main.py --tenant_id <your-tenant-id> --client_id <your-client-id> --client_secret <your-client-secret> --account_name <your-account-name> --container_name <your-container-name> --file_path "path/to/your/file.csv" --date_column "date" --target_date "2024-01-01" --filters '{"column": "age", "operator": ">", "value": 30}'
   ```

2. **Download a file from Azure Blob Storage:**
   ```bash
   python main.py --tenant_id <your-tenant-id> --client_id <your-client-id> --client_secret <your-client-secret> --account_name <your-account-name> --container_name <your-container-name> --download_path "local/download/path" --file_path "path/in/blob/storage/file.csv"
   ```

3. **Upload a file to Azure Data Lake Storage Gen2:**
   ```bash
   python main.py --tenant_id <your-tenant-id> --client_id <your-client-id> --client_secret <your-client-secret> --account_name <your-account-name> --upload_path "path/in/datalake/file.csv" --local_file "local/file/path.csv"
   ```

### Unit Tests

The project includes unit tests to ensure the reliability of the codebase.

1. **Run the tests:**
   ```bash
   coverage run -m unittest discover -s tests
   ```

2. **View the coverage report:**
   ```bash
   coverage report -m
   ```

## Contact

For any questions or issues, please contact [evangelos.patsourakos@ubs.com](evangelos.patsourakos@ubs.com).
