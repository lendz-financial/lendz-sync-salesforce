import logging

def helper_code() -> None:
    logging.info("Helper code function successfully executed. AB")
    
import csv
from io import StringIO, BytesIO
import os
import requests
import pyodbc
from simple_salesforce import Salesforce, SalesforceError
import pandas as pd
import datetime 

# Azure Blob Storage imports
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError

def _execute_db_batch(cursor, cnxn, batch_data):
    """
    Executes a batch update for Azure SQL Database.
    batch_data is a list of tuples: [(azure_blob_url, content_document_id, system_modstamp, content_version_id), ...]
    Returns the number of rows affected or -1 on failure.
    """
    if not batch_data:
        return 0 

    values_placeholders = ', '.join(['(?, ?)' for _ in batch_data]) 
    
    flat_params = []
    for url, doc_id, _, _ in batch_data: # Ignore system_modstamp and content_version_id here
        flat_params.append(url)
        flat_params.append(doc_id)

    update_sql = f"""
    UPDATE T
    SET T.AzureBlobUrl = V.AzureBlobUrl
    FROM [dbo].[ContentVersion] AS T
    JOIN (VALUES {values_placeholders}) AS V(AzureBlobUrl, ContentDocumentId)
        ON T.ContentDocumentId = V.ContentDocumentId;
    """
    
    try:
        cursor.execute(update_sql, *flat_params) 
        cnxn.commit()
        return cursor.rowcount 
    except pyodbc.Error as sql_err:
        cnxn.rollback()
        print(f"  ERROR executing batch SQL DB update: {sql_err}")
        return -1 

def _update_sync_state(cursor, cnxn, state_name, last_record_id, last_system_modstamp):
    """
    Updates the SyncState table with the latest processed record information.
    Uses MERGE for an UPSERT operation.
    last_record_id is now a generic placeholder for the ID of any Salesforce object.
    """
    sync_state_sql = """
    MERGE [dbo].[SyncState] AS T
    USING (SELECT ? AS StateName, ? AS LastRecordId, ? AS LastSystemModstamp) AS S
    ON T.StateName = S.StateName
    WHEN MATCHED THEN
        UPDATE SET
            T.LastRecordId = S.LastRecordId,
            T.LastSystemModstamp = S.LastSystemModstamp,
            T.LastUpdatedDateTime = SYSDATETIMEOFFSET()
    WHEN NOT MATCHED THEN
        INSERT (StateName, LastRecordId, LastSystemModstamp)
        VALUES (S.StateName, S.LastRecordId, S.LastSystemModstamp);
    """
    try:
        cursor.execute(sync_state_sql, state_name, last_record_id, last_system_modstamp)
        cnxn.commit()
        print(f"  SyncState updated for '{state_name}' to Record ID: {last_record_id}, Modstamp: {last_system_modstamp}")
        return True
    except pyodbc.Error as sql_err:
        cnxn.rollback()
        print(f"  ERROR updating SyncState for '{state_name}': {sql_err}")
        return False


def download_content_versions_and_files_to_azure_blob_and_sql_batched(
    username, password, security_token, last_sync_timestamp, 
    sandbox=False
):
    """
    Downloads ContentVersion objects from Salesforce using Bulk API 2.0,
    uploads files to Azure Blob Storage, and updates Azure SQL Database in batches.
    Also updates a SyncState table with the last processed record's SystemModstamp.

    Required Environment Variables:
    - SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN (for Salesforce)
    - AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_CONTAINER_NAME, AZURE_STORAGE_ACCOUNT_KEY (for Azure Blob)
    - AZURE_SQL_CONNECTION_STRING (for Azure SQL DB)
    - AZURE_DB_BATCH_SIZE (optional, defaults to 50 if not set or invalid)

    Args:
        username (str): Salesforce username.
        password (str): Salesforce password.
        security_token (str): Salesforce security token.
        last_sync_timestamp (str): Timestamp string (e.g., '2024-01-01T00:00:00Z')
                                   to filter records by SystemModstamp greater than this.
        sandbox (bool, optional): Set to True if connecting to a sandbox environment.
                                  Defaults to False (production).

    Returns:
        pandas.DataFrame or list: A Pandas DataFrame containing the processed records
                                  (including Azure Blob URL and SQL update status),
                                  if pandas is installed, otherwise a list of dictionaries.
                                  Returns None if a critical error occurs.
    """
    sf = None
    container_client = None
    cnxn = None

    try:
        # --- Read Batch Size from Environment Variable ---
        db_batch_size_str = os.environ.get('AZURE_DB_BATCH_SIZE', '50') 
        try:
            db_batch_size = int(db_batch_size_str)
            if db_batch_size <= 0:
                raise ValueError("Batch size must be a positive integer.")
        except ValueError as e:
            print(f"Warning: Invalid AZURE_DB_BATCH_SIZE environment variable '{db_batch_size_str}'. Defaulting to 50. Error: {e}")
            db_batch_size = 50 

        # --- Salesforce Connection ---
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain='test' if sandbox else 'login'
        )
        print(f"Successfully connected to Salesforce. API version: {sf.api_version}")

        # --- Azure Blob Storage Setup ---
        azure_storage_account_name = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
        azure_storage_container_name = os.environ.get('AZURE_STORAGE_CONTAINER_NAME')
        azure_storage_account_key = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY')
        azure_storage_connection_string_blob = os.environ.get('AZURE_STORAGE_CONNECTION_STRING') 

        if not azure_storage_account_name or not azure_storage_container_name:
            raise ValueError(
                "Environment variables AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_CONTAINER_NAME must be set."
            )
        if not azure_storage_account_key and not azure_storage_connection_string_blob:
             raise ValueError(
                "Either AZURE_STORAGE_ACCOUNT_KEY or AZURE_STORAGE_CONNECTION_STRING (for Blob) must be set."
            )

        print("Initializing Azure Blob Storage client...")
        if azure_storage_connection_string_blob:
            blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string_blob)
        else:
            account_url = f"https://{azure_storage_account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=azure_storage_account_key)

        container_client = blob_service_client.get_container_client(azure_storage_container_name)
        try:
            container_client.get_container_properties()
            print(f"Connected to existing Azure container: {azure_storage_container_name}")
        except ResourceNotFoundError:
            print(f"Container '{azure_storage_container_name}' not found. Creating it now...")
            container_client.create_container()
            print(f"Container '{azure_storage_container_name}' created.")
        except ClientAuthenticationError as auth_err:
            raise ValueError(f"Azure authentication error for Blob Storage: {auth_err}")

        # --- Azure SQL Database Setup ---
        azure_sql_connection_string = os.environ.get('AZURE_SQL_CONNECTION_STRING')

        if not azure_sql_connection_string:
            raise ValueError(
                "Environment variable AZURE_SQL_CONNECTION_STRING must be set for Azure SQL Database connection."
            )
        
        print("Connecting to Azure SQL Database using connection string...")
        cnxn = pyodbc.connect(azure_sql_connection_string)
        cursor = cnxn.cursor()
        print("Successfully connected to Azure SQL Database.")

        # --- Salesforce SOQL Query ---
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

        # --- Execute Bulk API Query and Process Records ---
        job_result = sf.bulk.ContentVersion.query(soql_query)

        session_id = sf.session_id
        processed_records = []
        # db_update_batch now stores (url, doc_id, system_modstamp, content_version_id) tuples
        db_update_batch = [] 
        
        download_count = 0
        record_count = 0
        sql_batch_update_count = 0

        print(f"Starting file downloads, Azure Blob uploads, and Azure SQL updates (batch size: {db_batch_size})...")
        for record in job_result:
            record_count += 1
            version_data_url = record.get('VersionDataUrl')
            file_extension = record.get('FileExtension')
            title = record.get('Title')
            content_version_id = record.get('Id') # This is the ContentVersion.Id
            content_document_id = record.get('ContentDocumentId')
            system_modstamp = record.get('SystemModstamp') 

            record['AzureBlobUrl'] = None
            record['DownloadError'] = None
            record['SqlUpdateStatus'] = 'Skipped' 
            record['LastSystemModstampInBatch'] = None 

            if version_data_url and content_document_id and system_modstamp: 
                full_download_url = version_data_url 
                
                safe_title = "".join([c for c in (title or content_version_id) if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
                blob_name = f"{safe_title}"
                if file_extension:
                    blob_name = f"{blob_name}.{file_extension}"
                else:
                    blob_name = f"{blob_name}.bin"
                blob_name = blob_name.replace(' ', '_') 
                blob_name = f"{content_version_id}_{blob_name}"

                azure_blob_url = f"https://{azure_storage_account_name}.blob.core.windows.net/{azure_storage_container_name}/{blob_name}"

                try:
                    # --- Download File ---
                    headers = {
                        'Authorization': f'Bearer {session_id}'
                    }
                    response = requests.get(full_download_url, headers=headers, stream=True)
                    response.raise_for_status()

                    file_content_buffer = BytesIO()
                    for chunk in response.iter_content(chunk_size=8192):
                        file_content_buffer.write(chunk)
                    file_content_buffer.seek(0)

                    # --- Upload to Azure Blob ---
                    blob_client = container_client.get_blob_client(blob_name)
                    blob_client.upload_blob(file_content_buffer, overwrite=True)
                    
                    print(f"  Uploaded: {blob_name} (ID: {content_version_id}) to {azure_blob_url}")
                    record['AzureBlobUrl'] = azure_blob_url
                    download_count += 1
                    record['DownloadError'] = 'None' 

                    # --- Add to SQL DB Update Batch ---
                    # Store ContentVersion.Id as the record ID for sync state
                    db_update_batch.append((azure_blob_url, content_document_id, system_modstamp, content_version_id)) 
                    record['SqlUpdateStatus'] = 'Pending Batch Update'

                    # --- Execute Batch Update if size reached ---
                    if len(db_update_batch) >= db_batch_size:
                        print(f"  Executing batch update for {len(db_update_batch)} records...")
                        rows_affected = _execute_db_batch(cursor, cnxn, db_update_batch)
                        
                        if rows_affected >= 0: # ContentVersion batch update successful
                            sql_batch_update_count += rows_affected
                            
                            max_modstamp_in_batch = None
                            last_record_id_in_batch = None # Changed variable name
                            
                            for batch_item in db_update_batch:
                                # batch_item is (azure_blob_url, content_document_id, system_modstamp_str, content_version_id)
                                current_modstamp_str = batch_item[2]
                                current_record_id = batch_item[3] # Changed variable name

                                if max_modstamp_in_batch is None:
                                    max_modstamp_in_batch = datetime.datetime.fromisoformat(current_modstamp_str.replace('Z', '+00:00'))
                                    last_record_id_in_batch = current_record_id
                                else:
                                    compare_modstamp = datetime.datetime.fromisoformat(current_modstamp_str.replace('Z', '+00:00'))
                                    if compare_modstamp > max_modstamp_in_batch:
                                        max_modstamp_in_batch = compare_modstamp
                                        last_record_id_in_batch = current_record_id

                            # Update SyncState table with the progress
                            _update_sync_state(cursor, cnxn, 'ContentVersionSync', 
                                               last_record_id_in_batch, # Pass generic record ID
                                               max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')) 

                            for r in processed_records: 
                                if r.get('SqlUpdateStatus') == 'Pending Batch Update': 
                                    r['SqlUpdateStatus'] = 'Success (Batched)'
                                    r['LastSystemModstampInBatch'] = max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                        else: 
                             for r in processed_records:
                                if r.get('SqlUpdateStatus') == 'Pending Batch Update': 
                                    r['SqlUpdateStatus'] = 'Failed (Batched)' 
                        db_update_batch = [] 

                except requests.exceptions.RequestException as req_e:
                    print(f"  ERROR downloading/uploading {blob_name} (ID: {content_version_id}): {req_e}")
                    record['DownloadError'] = str(req_e)
                    record['SqlUpdateStatus'] = 'Not Attempted (Download Failed)'
                except Exception as e:
                    print(f"  UNEXPECTED ERROR for {blob_name} (ID: {content_version_id}): {e}")
                    record['DownloadError'] = str(e)
                    record['SqlUpdateStatus'] = 'Not Attempted (Processing Failed)'
            else:
                reason = ""
                if not version_data_url: reason += "No VersionDataUrl. "
                if not content_document_id: reason += "No ContentDocumentId. "
                if not system_modstamp: reason += "No SystemModstamp. "
                record['DownloadError'] = f"Skipped: {reason.strip()}"
                record['SqlUpdateStatus'] = 'Not Attempted (Missing Data)'
            
            processed_records.append(record)

        # --- Execute any remaining batch updates after loop ---
        if db_update_batch:
            print(f"  Executing final batch update for {len(db_update_batch)} records...")
            rows_affected = _execute_db_batch(cursor, cnxn, db_update_batch)
            
            if rows_affected >= 0: 
                sql_batch_update_count += rows_affected
                
                max_modstamp_in_batch = None
                last_record_id_in_batch = None # Changed variable name
                for batch_item in db_update_batch:
                    current_modstamp_str = batch_item[2]
                    current_record_id = batch_item[3] # Changed variable name

                    if max_modstamp_in_batch is None:
                        max_modstamp_in_batch = datetime.datetime.fromisoformat(current_modstamp_str.replace('Z', '+00:00'))
                        last_record_id_in_batch = current_record_id
                    else:
                        compare_modstamp = datetime.datetime.fromisoformat(current_modstamp_str.replace('Z', '+00:00'))
                        if compare_modstamp > max_modstamp_in_batch:
                            max_modstamp_in_batch = compare_modstamp
                            last_record_id_in_batch = current_record_id

                # Update SyncState table with the progress
                _update_sync_state(cursor, cnxn, 'ContentVersionSync', 
                                   last_record_id_in_batch, # Pass generic record ID
                                   max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z'))

                for r in processed_records: 
                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                        r['SqlUpdateStatus'] = 'Success (Batched - Final)'
                        r['LastSystemModstampInBatch'] = max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            else:
                for r in processed_records:
                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                        r['SqlUpdateStatus'] = 'Failed (Batched - Final)'
            
        if not processed_records:
            print("No ContentVersion records found since the last sync timestamp, or none processed.")
            return None

        print(f"Summary:")
        print(f"Total ContentVersion metadata records processed: {record_count}")
        print(f"Total files uploaded to Azure Blob: {download_count}")
        print(f"Total SQL DB records updated via batches: {sql_batch_update_count}")

        if 'pd' in globals():
            df = pd.DataFrame(processed_records)
            return df
        else:
            return processed_records

    except (SalesforceError, ValueError, ClientAuthenticationError, pyodbc.Error) as e:
        print(f"A critical error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected general error occurred: {e}")
        return None
    finally:
        if sf and hasattr(sf, 'session') and sf.session:
            try:
                sf.close()
                print("Salesforce session closed.")
            except Exception as e:
                print(f"Error closing Salesforce session: {e}")
        if cnxn:
            try:
                cnxn.close()
                print("Azure SQL Database connection closed.")
            except Exception as e:
                print(f"Error closing Azure SQL Database connection: {e}")

# --- Example Usage ---
if __name__ == "__main__":
    # --- Set your environment variables before running this script ---
    # For Salesforce:
    # export SF_USERNAME="your_salesforce_username"
    # export SF_PASSWORD="your_salesforce_password"
    # export SF_SECURITY_TOKEN="your_security_token"
    
    # For Azure Blob Storage:
    # export AZURE_STORAGE_ACCOUNT_NAME="your_storage_account_name"
    # export AZURE_STORAGE_CONTAINER_NAME="your_container_name"
    # export AZURE_STORAGE_ACCOUNT_KEY="your_storage_account_key" # OR AZURE_STORAGE_CONNECTION_STRING (for Blob)
    
    # For Azure SQL Database:
    # export AZURE_SQL_CONNECTION_STRING="DRIVER={ODBC Driver 17 for SQL Server};SERVER=yourserver.database.windows.net;DATABASE=yourdatabase;UID=yourusername;PWD=yourpassword"
    # export AZURE_DB_BATCH_SIZE="50" # Optional, defaults to 50

    SF_USERNAME = os.environ.get('SF_USERNAME')
    SF_PASSWORD = os.environ.get('SF_PASSWORD')
    SF_SECURITY_TOKEN = os.environ.get('SF_SECURITY_TOKEN')
    
    # IMPORTANT: In a real scenario, you would fetch this from the SyncState table
    # Example: query SyncState table for 'ContentVersionSync' and get LastSystemModstamp
    # If not found or NULL, use a default start date.
    LAST_SYNC_TIMESTAMP = '2024-01-01T00:00:00Z' 
    IS_SANDBOX = False 

    print(f"Starting process to download ContentVersion, upload to Azure Blob, and update Azure SQL DB in batches, for records modified after: {LAST_SYNC_TIMESTAMP}")
    
    results = download_content_versions_and_files_to_azure_blob_and_sql_batched(
        SF_USERNAME, 
        SF_PASSWORD, 
        SF_SECURITY_TOKEN, 
        LAST_SYNC_TIMESTAMP, 
        sandbox=IS_SANDBOX
    )

    if results is not None:
        if isinstance(results, pd.DataFrame):
            print("\nFinal Processed Data (first 5 rows):")
            print(results[['Id', 'Title', 'FileExtension', 'AzureBlobUrl', 'SqlUpdateStatus', 'LastSystemModstampInBatch', 'DownloadError']].head())
            print(f"\nTotal records processed by script: {len(results)}")
        else:
            print("\nFinal Processed Data (first record with status):")
            if results:
                print(results[0])
            else:
                print("No records to display.")
            print(f"\nTotal records processed by script: {len(results)}")
    else:
        print("Script terminated due to a critical error.")