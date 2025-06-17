import logging

def helper_code() -> None:
    logging.info("Helper code function successfully executed. AB")
    
import csv
from io import StringIO, BytesIO # Added BytesIO
import os
import requests
from simple_salesforce import Salesforce, SalesforceError
import pandas as pd # Optional: for easier data handling

# Azure Blob Storage imports
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError

def download_content_versions_and_files_to_azure_blob(
    username, password, security_token, last_sync_timestamp, 
    sandbox=False
):
    """
    Downloads ContentVersion objects from Salesforce using Bulk API 2.0
    with a SystemModstamp greater than the provided timestamp,
    and then uploads the associated files to Azure Blob Storage.

    Azure Storage Account Name and Container Name must be set as environment variables:
    - AZURE_STORAGE_ACCOUNT_NAME
    - AZURE_STORAGE_CONTAINER_NAME
    - AZURE_STORAGE_ACCOUNT_KEY (or AZURE_STORAGE_CONNECTION_STRING)

    Args:
        username (str): Salesforce username.
        password (str): Salesforce password.
        security_token (str): Salesforce security token.
        last_sync_timestamp (str): Timestamp string (e.g., '2024-01-01T00:00:00Z')
                                   to filter records by SystemModstamp greater than this.
        sandbox (bool, optional): Set to True if connecting to a sandbox environment.
                                  Defaults to False (production).

    Returns:
        pandas.DataFrame or list: A Pandas DataFrame containing the downloaded records
                                  (including the Azure Blob URL if successful),
                                  if pandas is installed, otherwise a list of dictionaries.
                                  Returns None if an error occurs.
    """
    sf = None # Initialize sf to None
    container_client = None # Initialize container_client to None

    try:
        # 1. Authenticate with Salesforce
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain='test' if sandbox else 'login'
        )
        print(f"Successfully connected to Salesforce. API version: {sf.api_version}")

        # 2. Get Azure Blob Storage credentials from environment variables
        azure_storage_account_name = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
        azure_storage_container_name = os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
        azure_storage_account_key = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY') # Prefer account key for simplicity here
        azure_storage_connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

        if not azure_storage_account_name or not azure_storage_container_name:
            raise ValueError(
                "Environment variables AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_CONTAINER_NAME must be set."
            )
        if not azure_storage_account_key and not azure_storage_connection_string:
             raise ValueError(
                "Either AZURE_STORAGE_ACCOUNT_KEY or AZURE_STORAGE_CONNECTION_STRING must be set."
            )

        # 3. Initialize Azure Blob Service Client
        print("Initializing Azure Blob Storage client...")
        if azure_storage_connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        else:
            # Construct the account URL from the account name
            account_url = f"https://{azure_storage_account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=azure_storage_account_key)

        # Get a client to interact with the specific container
        container_client = blob_service_client.get_container_client(azure_storage_container_name)
        
        # Check if container exists, create if not (optional, but good for first run)
        try:
            container_client.get_container_properties()
            print(f"Connected to existing Azure container: {azure_storage_container_name}")
        except ResourceNotFoundError:
            print(f"Container '{azure_storage_container_name}' not found. Creating it now...")
            container_client.create_container()
            print(f"Container '{azure_storage_container_name}' created.")
        except ClientAuthenticationError as auth_err:
            raise ValueError(f"Azure authentication error: Check your storage account name/key/connection string. Details: {auth_err}")


        # 4. Construct the SOQL query
        soql_query = (
            f"SELECT Id, ContentDocumentId, IsLatest, ContentUrl, ContentBodyId, "
            f"VersionNumber, Title, Description, ReasonForChange, SharingOption, "
            f"SharingPrivacy, PathOnClient, RatingCount, IsDeleted, ContentModifiedDate, "
            f"ContentModifiedById, PositiveRatingCount, NegativeRatingCount, FeaturedContentBoost, "
            f"FeaturedContentDate, OwnerId, CreatedById, CreatedDate, LastModifiedById, "
            f"LastModifiedDate, SystemModstamp, TagCsv, FileType, PublishStatus, "
            f"ContentSize, FileExtension, FirstPublishLocationId, Origin, NetworkId, "
            f"ContentLocation, TextPreview, ExternalDocumentInfo1, ExternalDocumentInfo2, "
            f"Checksum, IsMajorVersion, IsAssetEnabled, VersionDataUrl "
            f"FROM ContentVersion WHERE SystemModstamp > {last_sync_timestamp}"
        )
        print(f"Executing SOQL query: {soql_query}")

        # 5. Execute the Bulk API query (Bulk 2.0)
        job_result = sf.bulk.ContentVersion.query(soql_query)

        session_id = sf.session_id
        processed_records = []
        download_count = 0
        record_count = 0

        print("Starting file downloads and Azure Blob uploads...")
        # 6. Iterate directly through job_result and upload files to Azure
        for record in job_result:
            record_count += 1
            version_data_url = record.get('VersionDataUrl')
            file_extension = record.get('FileExtension')
            title = record.get('Title')
            content_version_id = record.get('Id')

            if version_data_url:
                full_download_url = version_data_url # Using VersionDataUrl directly as the full URL
                
                # Sanitize filename for common OS/URL compatibility issues and use as blob_name
                # Azure Blob names are case-sensitive and can contain most characters,
                # but it's good practice to avoid ones that are problematic in URLs or OS paths.
                safe_title = "".join([c for c in (title or content_version_id) if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
                blob_name = f"{safe_title}"
                if file_extension:
                    blob_name = f"{blob_name}.{file_extension}"
                else:
                    blob_name = f"{blob_name}.bin" # Generic binary extension

                # Replace spaces with underscores or dashes for cleaner blob names in URLs
                blob_name = blob_name.replace(' ', '_') 
                # Add ContentVersion ID to ensure uniqueness, useful for versions with same title
                blob_name = f"{content_version_id}_{blob_name}"

                azure_blob_url = f"https://{azure_storage_account_name}.blob.core.windows.net/{azure_storage_container_name}/{blob_name}"

                try:
                    headers = {
                        'Authorization': f'Bearer {session_id}'
                    }
                    
                    response = requests.get(full_download_url, headers=headers, stream=True)
                    response.raise_for_status()

                    # Use BytesIO to buffer the file content in memory
                    file_content_buffer = BytesIO()
                    for chunk in response.iter_content(chunk_size=8192):
                        file_content_buffer.write(chunk)
                    
                    # Reset buffer position to the beginning before uploading
                    file_content_buffer.seek(0)

                    # Upload the buffered content to Azure Blob
                    blob_client = container_client.get_blob_client(blob_name)
                    blob_client.upload_blob(file_content_buffer, overwrite=True) # overwrite=True allows re-uploading same file name
                    
                    print(f"  Uploaded: {blob_name} (ID: {content_version_id}) to {azure_blob_url}")
                    record['AzureBlobUrl'] = azure_blob_url # Store the Azure Blob URL
                    download_count += 1
                except requests.exceptions.RequestException as req_e:
                    print(f"  Error downloading {blob_name} (ID: {content_version_id}) from {full_download_url}: {req_e}")
                    record['DownloadError'] = str(req_e)
                except Exception as e:
                    print(f"  An unexpected error occurred during upload of {blob_name} (ID: {content_version_id}) to Azure: {e}")
                    record['DownloadError'] = str(e)
            else:
                record['DownloadError'] = "No VersionDataUrl"
            
            processed_records.append(record)

        if not processed_records:
            print("No ContentVersion records found since the last sync timestamp, or none processed.")
            return None

        print(f"Total ContentVersion metadata records processed: {record_count}")
        print(f"Total files uploaded to Azure Blob: {download_count}")

        # 7. Process the results (optional: convert to DataFrame)
        if 'pd' in globals():
            df = pd.DataFrame(processed_records)
            return df
        else:
            return processed_records

    except (SalesforceError, ValueError, ClientAuthenticationError) as e:
        print(f"An error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected general error occurred: {e}")
        return None
    finally:
        # Close the Salesforce session if it was opened
        if sf and hasattr(sf, 'session') and sf.session:
            try:
                sf.close()
                print("Salesforce session closed.")
            except Exception as e:
                print(f"Error closing Salesforce session: {e}")

# --- Example Usage ---
if __name__ == "__main__":
    # Set these environment variables before running the script:
    # export AZURE_STORAGE_ACCOUNT_NAME="your_storage_account_name"
    # export AZURE_STORAGE_CONTAINER_NAME="your_container_name"
    # export AZURE_STORAGE_ACCOUNT_KEY="your_storage_account_key"
    # OR export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=..."

    SF_USERNAME = os.environ.get('SF_USERNAME', 'your_salesforce_username')
    SF_PASSWORD = os.environ.get('SF_PASSWORD', 'your_salesforce_password')
    SF_SECURITY_TOKEN = os.environ.get('SF_SECURITY_TOKEN', 'your_security_token')
    
    LAST_SYNC_TIMESTAMP = '2025-06-15T00:00:00Z' # Adjust as needed
    
    IS_SANDBOX = False 

    print(f"Attempting to download ContentVersion records and upload files to Azure Blob Storage, modified after: {LAST_SYNC_TIMESTAMP}")
    
    content_versions_uploaded = download_content_versions_and_files_to_azure_blob(
        SF_USERNAME, 
        SF_PASSWORD, 
        SF_SECURITY_TOKEN, 
        LAST_SYNC_TIMESTAMP, 
        sandbox=IS_SANDBOX
    )

    if content_versions_uploaded is not None:
        if isinstance(content_versions_uploaded, pd.DataFrame):
            print("\nProcessed ContentVersion Data with Azure Blob URLs (first 5 rows):")
            print(content_versions_uploaded[['Id', 'Title', 'FileExtension', 'AzureBlobUrl', 'DownloadError']].head())
            print(f"\nTotal records processed: {len(content_versions_uploaded)}")
        else:
            print("\nProcessed ContentVersion Data (first record with Azure Blob URL):")
            if content_versions_uploaded:
                print(content_versions_uploaded[0])
            else:
                print("No records to display.")
            print(f"\nTotal records processed: {len(content_versions_uploaded)}")
    else:
        print("Failed to download ContentVersion records or upload files to Azure Blob Storage.")