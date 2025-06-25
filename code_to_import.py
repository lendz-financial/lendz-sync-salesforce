import logging
import csv
from io import StringIO, BytesIO
import os
import requests
import pyodbc
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from simple_salesforce import Salesforce, SalesforceError
from datetime import datetime, timezone, timedelta

def helper_code() -> None:
    logging.info("Helper code function successfully executed. AB")
    

def _execute_db_batch(cursor, cnxn, batch_data):
    """
    Executes a batch update for Azure SQL Database (ContentVersion table).
    batch_data is a list of tuples: [(azure_blob_url, content_document_id, system_modstamp, content_version_id), ...]
    Returns the number of rows affected or -1 on failure.
    """
    if not batch_data:
        return 0 

    values_placeholders = ', '.join(['(?, ?)' for _ in batch_data]) 
    
    flat_params = []
    for url, doc_id, _, _ in batch_data: 
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
        print(f"    ERROR executing batch SQL DB update for ContentVersion: {sql_err}")
        return -1 

def _execute_cdl_db_batch(cursor, cnxn, batch_data):
    """
    Executes a batch MERGE operation for Azure SQL Database (ContentDocumentLink table).
    batch_data is a list of tuples: [(Id, LinkedEntityId, ContentDocumentId, IsDeleted, SystemModstamp, ShareType, Visibility), ...]
    Returns the number of rows affected or -1 on failure.
    """
    if not batch_data:
        return 0

    # Each tuple in batch_data has 7 elements
    values_placeholders = ', '.join(['(?, ?, ?, ?, ?, ?, ?)' for _ in batch_data])

    flat_params = []
    for item in batch_data:
        # Extend with each element of the tuple
        flat_params.extend(item)

    merge_sql = f"""
    MERGE [dbo].[ContentDocumentLink] AS T
    USING (VALUES {values_placeholders}) AS S
        (Id, LinkedEntityId, ContentDocumentId, IsDeleted, SystemModstamp, ShareType, Visibility)
    ON T.Id = S.Id
    WHEN MATCHED AND T.SystemModstamp < S.SystemModstamp THEN -- Only update if source is newer
        UPDATE SET
            T.LinkedEntityId = S.LinkedEntityId,
            T.ContentDocumentId = S.ContentDocumentId,
            T.IsDeleted = S.IsDeleted,
            T.SystemModstamp = S.SystemModstamp,
            T.ShareType = S.ShareType,
            T.Visibility = S.Visibility
    WHEN NOT MATCHED THEN
        INSERT (Id, LinkedEntityId, ContentDocumentId, IsDeleted, SystemModstamp, ShareType, Visibility)
        VALUES (S.Id, S.LinkedEntityId, S.ContentDocumentId, S.IsDeleted, S.SystemModstamp, S.ShareType, S.Visibility);
    """
    
    try:
        cursor.execute(merge_sql, *flat_params)
        cnxn.commit()
        return cursor.rowcount
    except pyodbc.Error as sql_err:
        cnxn.rollback()
        print(f"    ERROR executing batch SQL DB MERGE for ContentDocumentLink: {sql_err}")
        return -1

def _update_sync_state(cursor, cnxn, state_name, last_record_id, last_system_modstamp):
    """
    Updates the SyncState table with the latest processed record information.
    Uses MERGE for an UPSERT operation.
    last_record_id is now a generic placeholder for the ID of any Salesforce object.
    last_system_modstamp should be in Salesforce's Besançon-MM-DDTHH:MM:SS.sssZ format.
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
        print(f"    SyncState updated for '{state_name}' to Record ID: {last_record_id}, Modstamp: {last_system_modstamp}")
        return True
    except pyodbc.Error as sql_err:
        cnxn.rollback()
        print(f"    ERROR updating SyncState for '{state_name}': {sql_err}")
        return False

def _get_last_sync_timestamp_from_db(cursor, state_name):
    """
    Retrieves the LastSystemModstamp for a given state_name from the SyncState table.
    It converts the stored DATETIMEOFFSET to UTC DATETIME.
    Returns the timestamp in Salesforce's Besançon-MM-DDTHH:MM:SS.sssZ format, or None if not found.
    """
    # SQL query applying Option 1: Convert LastSystemModstamp to UTC DATETIME
    select_sql = """
    SELECT
        CAST(LastSystemModstamp AT TIME ZONE 'UTC' AS DATETIME) AS LastSystemModstampUtcDateTime
    FROM
        [dbo].[SyncState]
    WHERE
        StateName = ?
    """
    try:
        cursor.execute(select_sql, state_name)
        result = cursor.fetchone()
        if result and result[0]:
            dt_object = result[0]
            # Convert datetime object (which is now guaranteed to be UTC from the SQL query)
            # back to Salesforce's expected string format (ISO 8601 with 'Z' for UTC)
            # Ensure proper ISO format for timezone-aware or naive UTC datetime
            if dt_object.tzinfo is not None and dt_object.tzinfo.utcoffset(dt_object) == timedelta(0):
                    # If it's a timezone-aware UTC datetime (e.g., from datetime.fromisoformat with +00:00)
                return dt_object.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            else:
                # If it's a naive datetime (typical for CAST AS DATETIME), assume it's UTC and append 'Z'
                return dt_object.isoformat(timespec='milliseconds') + 'Z'
        else:
            print(f"    No existing sync state found for '{state_name}' in SyncState table.")
            return None
    except pyodbc.Error as sql_err:
        print(f"    ERROR retrieving last sync timestamp for '{state_name}': {sql_err}")
        return None


def download_content_versions_and_files_to_azure_blob_and_sql_batched(
    username, password, security_token, initial_last_sync_timestamp, 
    sandbox=False
):
    """
    Downloads ContentVersion objects from Salesforce using Bulk API 2.0,
    uploads files to Azure Blob Storage, and updates Azure SQL Database in batches.
    Starts download from the LastSystemModstamp recorded in SyncState table.

    Required Environment Variables:
    - SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN (for Salesforce)
    - AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_CONTAINER_NAME, AZURE_STORAGE_ACCOUNT_KEY (for Azure Blob)
    - AZURE_SQL_CONNECTION_STRING (for Azure SQL DB)
    - AZURE_DB_BATCH_SIZE (optional, defaults to 5 if not set or invalid)

    Args:
        username (str): Salesforce username.
        password (str): Salesforce password.
        security_token (str): Salesforce security token.
        initial_last_sync_timestamp (str): Fallback timestamp string (e.g., '2024-01-01T00:00:00Z')
                                            to use if no state is found in the database.
        sandbox (bool, optional): Set to True if connecting to a sandbox environment.
                                  Defaults to False (production).

    Returns:
        list: A list of dictionaries containing the processed records
              (including Azure Blob URL and SQL update status).
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
            print(f"Warning: Invalid AZURE_DB_BATCH_SIZE environment variable '{db_batch_size_str}'. Defaulting to 5. Error: {e}")
            db_batch_size = 5

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

        # --- Determine the actual start timestamp for the SOQL query ---
        db_last_sync_timestamp = _get_last_sync_timestamp_from_db(cursor, 'ContentVersionSync')
        
        soql_start_timestamp = db_last_sync_timestamp if db_last_sync_timestamp else initial_last_sync_timestamp
        print(f"Starting Salesforce ContentVersion query from SystemModstamp: {soql_start_timestamp}")


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
            f"FROM ContentVersion WHERE SystemModstamp > {soql_start_timestamp} "
            f"ORDER BY SystemModstamp ASC" # Added ORDER BY clause
        )
        print(f"Executing SOQL query: {soql_query}")

        # --- Execute Bulk API Query and Process Records ---
        job_result = sf.bulk.ContentVersion.query(soql_query)

        session_id = sf.session_id
        processed_records = []
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
            content_version_id = record.get('Id') 
            content_document_id = record.get('ContentDocumentId')
            system_modstamp = record.get('SystemModstamp') # This is the incoming long millisecond timestamp

            record['AzureBlobUrl'] = None
            record['DownloadError'] = None
            record['SqlUpdateStatus'] = 'Skipped' 
            record['LastSystemModstampInBatch'] = None 

            if version_data_url and content_document_id and system_modstamp is not None: # Check for None explicitly
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
                    
                    print(f"    Uploaded: {blob_name} (ID: {content_version_id}) to {azure_blob_url}")
                    record['AzureBlobUrl'] = azure_blob_url
                    record['DownloadError'] = 'None' 

                    # --- Add to SQL DB Update Batch ---
                    # system_modstamp (from record.get) is the millisecond timestamp here
                    # We store it in db_update_batch as is for now, it's converted to ISO string later
                    # for the DB update itself.
                    db_update_batch.append((azure_blob_url, content_document_id, system_modstamp, content_version_id)) 
                    record['SqlUpdateStatus'] = 'Pending Batch Update'

                    # --- Execute Batch Update if size reached ---
                    if len(db_update_batch) >= db_batch_size:
                        print(f"    Executing ContentVersion batch update for {len(db_update_batch)} records...")
                        rows_affected = _execute_db_batch(cursor, cnxn, db_update_batch)
                        
                        if rows_affected >= 0: 
                            sql_batch_update_count += rows_affected
                            
                            max_modstamp_in_batch = None
                            last_record_id_in_batch = None 
                            
                            for batch_item in db_update_batch:
                                current_modstamp_ms = batch_item[2] # This is now the long millisecond timestamp
                                current_record_id = batch_item[3]

                                # Convert milliseconds since epoch to UTC datetime object
                                try:
                                    current_modstamp_dt = datetime.fromtimestamp(float(current_modstamp_ms) / 1000, tz=timezone.utc)
                                except (TypeError, ValueError) as e:
                                    print(f"    WARNING: Could not parse SystemModstamp '{current_modstamp_ms}' for record {current_record_id}. Error: {e}")
                                    continue # Skip this item for timestamp comparison, but still process others in batch

                                if max_modstamp_in_batch is None or current_modstamp_dt > max_modstamp_in_batch:
                                    max_modstamp_in_batch = current_modstamp_dt
                                    last_record_id_in_batch = current_record_id

                            # Only update sync state if a valid max timestamp was found in the batch
                            if max_modstamp_in_batch:
                                # Convert Python datetime object back to Salesforce's expected string format for DB storage
                                max_modstamp_sf_format = max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

                                _update_sync_state(cursor, cnxn, 'ContentVersionSync', 
                                                     last_record_id_in_batch, 
                                                     max_modstamp_sf_format) 

                                for r in processed_records: 
                                    if r.get('SqlUpdateStatus') == 'Pending Batch Update': 
                                        r['SqlUpdateStatus'] = 'Success (Batched)'
                                        r['LastSystemModstampInBatch'] = max_modstamp_sf_format
                            else:
                                print(f"    WARNING: No valid SystemModstamp found in batch for ContentVersion sync state update.")
                                for r in processed_records:
                                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                                        r['SqlUpdateStatus'] = 'Skipped (No valid timestamp in batch)'

                        else: # rows_affected < 0, indicating DB error
                             for r in processed_records:
                                 if r.get('SqlUpdateStatus') == 'Pending Batch Update': 
                                     r['SqlUpdateStatus'] = 'Failed (Batched)' 
                        db_update_batch = [] 

                except requests.exceptions.RequestException as req_e:
                    print(f"    ERROR downloading/uploading {blob_name} (ID: {content_version_id}): {req_e}")
                    record['DownloadError'] = str(req_e)
                    record['SqlUpdateStatus'] = 'Not Attempted (Download Failed)'
                except Exception as e:
                    print(f"    UNEXPECTED ERROR for {blob_name} (ID: {content_version_id}): {e}")
                    record['DownloadError'] = str(e)
                    record['SqlUpdateStatus'] = 'Not Attempted (Processing Failed)'
            else:
                reason = ""
                if not version_data_url: reason += "No VersionDataUrl. "
                if not content_document_id: reason += "No ContentDocumentId. "
                if system_modstamp is None: reason += "No SystemModstamp. "
                record['DownloadError'] = f"Skipped: {reason.strip()}"
                record['SqlUpdateStatus'] = 'Not Attempted (Missing Data)'
            
            processed_records.append(record)

        # --- Execute any remaining batch updates after loop ---
        if db_update_batch:
            print(f"    Executing final ContentVersion batch update for {len(db_update_batch)} records...")
            rows_affected = _execute_db_batch(cursor, cnxn, db_update_batch)
            
            if rows_affected >= 0: 
                sql_batch_update_count += rows_affected
                
                max_modstamp_in_batch = None
                last_record_id_in_batch = None 
                for batch_item in db_update_batch:
                    current_modstamp_ms = batch_item[2] # This is now the long millisecond timestamp
                    current_record_id = batch_item[3]

                    # Convert milliseconds since epoch to UTC datetime object
                    try:
                        current_modstamp_dt = datetime.fromtimestamp(float(current_modstamp_ms) / 1000, tz=timezone.utc)
                    except (TypeError, ValueError) as e:
                        print(f"    WARNING: Could not parse SystemModstamp '{current_modstamp_ms}' for record {current_record_id}. Error: {e}")
                        continue # Skip this item for timestamp comparison, but still process others in batch

                    if max_modstamp_in_batch is None or current_modstamp_dt > max_modstamp_in_batch:
                        max_modstamp_in_batch = current_modstamp_dt
                        last_record_id_in_batch = current_record_id

                # Only update sync state if a valid max timestamp was found in the batch
                if max_modstamp_in_batch:
                    max_modstamp_sf_format = max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

                    _update_sync_state(cursor, cnxn, 'ContentVersionSync', 
                                         last_record_id_in_batch, 
                                         max_modstamp_sf_format)

                    for r in processed_records: 
                        if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                            r['SqlUpdateStatus'] = 'Success (Batched - Final)'
                            r['LastSystemModstampInBatch'] = max_modstamp_sf_format
                else:
                    print(f"    WARNING: No valid SystemModstamp found in final ContentVersion batch for sync state update.")
                    for r in processed_records:
                        if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                            r['SqlUpdateStatus'] = 'Skipped (No valid timestamp in final batch)'

            else: # rows_affected < 0, indicating DB error
                for r in processed_records:
                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                        r['SqlUpdateStatus'] = 'Failed (Batched - Final)'
            
        if not processed_records:
            print(f"No ContentVersion records found since the last sync timestamp ({soql_start_timestamp}), or none processed.")
            return None

        print(f"Summary for ContentVersion sync:")
        print(f"Total ContentVersion metadata records processed: {record_count}")
        print(f"Total files uploaded to Azure Blob: {download_count}")
        print(f"Total SQL DB records updated via batches: {sql_batch_update_count}")

        return processed_records

    except (SalesforceError, ValueError, ClientAuthenticationError, pyodbc.Error) as e:
        print(f"A critical error occurred during ContentVersion sync: {e}")
        return None
    except Exception as e:
        print(f"An unexpected general error occurred during ContentVersion sync: {e}")
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


def download_content_document_links_to_sql_batched(
    username, password, security_token, initial_last_sync_timestamp,
    sandbox=False
):
    """
    Downloads ContentDocumentLink objects from Salesforce using Bulk API 2.0
    and stores/updates them in the Azure SQL Database.
    Starts download from the LastSystemModstamp recorded in SyncState table.

    Required Environment Variables:
    - SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN (for Salesforce)
    - AZURE_SQL_CONNECTION_STRING (for Azure SQL DB)
    - AZURE_DB_BATCH_SIZE (optional, defaults to 5 if not set or invalid)

    Args:
        username (str): Salesforce username.
        password (str): Salesforce password.
        security_token (str): Salesforce security token.
        initial_last_sync_timestamp (str): Fallback timestamp string (e.g., '2024-01-01T00:00:00Z')
                                            to use if no state is found in the database.
        sandbox (bool, optional): Set to True if connecting to a sandbox environment.
                                  Defaults to False (production).

    Returns:
        list: A list of dictionaries containing the processed records
              (including SQL update status).
              Returns None if a critical error occurs.
    """
    sf = None
    cnxn = None

    try:
        # --- Read Batch Size from Environment Variable ---
        db_batch_size_str = os.environ.get('AZURE_DB_BATCH_SIZE', '50') 
        try:
            db_batch_size = int(db_batch_size_str)
            if db_batch_size <= 0:
                raise ValueError("Batch size must be a positive integer.")
        except ValueError as e:
            print(f"Warning: Invalid AZURE_DB_BATCH_SIZE environment variable '{db_batch_size_str}'. Defaulting to 5. Error: {e}")
            db_batch_size = 5

        # --- Salesforce Connection ---
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain='test' if sandbox else 'login'
        )
        print(f"Successfully connected to Salesforce. API version: {sf.api_version}")

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

        # --- Determine the actual start timestamp for the SOQL query ---
        db_last_sync_timestamp = _get_last_sync_timestamp_from_db(cursor, 'ContentDocumentLinkSync')
        
        soql_start_timestamp = db_last_sync_timestamp if db_last_sync_timestamp else initial_last_sync_timestamp
        print(f"Starting Salesforce ContentDocumentLink query from SystemModstamp: {soql_start_timestamp}")

        # --- Salesforce SOQL Query ---
        # Select fields matching the [dbo].[ContentDocumentLink] table structure
        soql_query = (
            f"SELECT Id, LinkedEntityId, ContentDocumentId, IsDeleted, SystemModstamp, ShareType, Visibility "
            f"FROM ContentDocumentLink WHERE SystemModstamp > {soql_start_timestamp} "
            f"ORDER BY SystemModstamp ASC" # Added ORDER BY clause
        )
        print(f"Executing SOQL query: {soql_query}")

        # --- Execute Bulk API Query and Process Records ---
        job_result = sf.bulk.ContentDocumentLink.query(soql_query)

        processed_records = []
        db_update_batch = []
        
        record_count = 0
        sql_batch_update_count = 0

        print(f"Starting ContentDocumentLink Azure SQL updates (batch size: {db_batch_size})...")
        for record in job_result:
            record_count += 1
            cdl_id = record.get('Id')
            linked_entity_id = record.get('LinkedEntityId')
            content_document_id = record.get('ContentDocumentId')
            is_deleted = record.get('IsDeleted')
            # Changed variable name to reflect it's milliseconds (int)
            system_modstamp_ms = record.get('SystemModstamp') 

            share_type = record.get('ShareType')
            visibility = record.get('Visibility')

            # Initialize status fields for the processed_records list
            record['SqlUpdateStatus'] = 'Skipped'
            record['LastSystemModstampInBatch'] = None
            record['ProcessingError'] = None

            # Changed check to system_modstamp_ms
            if cdl_id and content_document_id and system_modstamp_ms is not None:
                try:
                    # Corrected: Convert milliseconds since epoch to UTC datetime object
                    system_modstamp_dt = datetime.fromtimestamp(float(system_modstamp_ms) / 1000, tz=timezone.utc)

                    # Add to SQL DB Update Batch
                    db_update_batch.append((
                        cdl_id, linked_entity_id, content_document_id,
                        is_deleted, system_modstamp_dt, share_type, visibility
                    ))
                    record['SqlUpdateStatus'] = 'Pending Batch Update'

                    # Execute Batch Update if size reached
                    if len(db_update_batch) >= db_batch_size:
                        print(f"    Executing ContentDocumentLink batch MERGE for {len(db_update_batch)} records...")
                        rows_affected = _execute_cdl_db_batch(cursor, cnxn, db_update_batch)
                        
                        if rows_affected >= 0:
                            sql_batch_update_count += rows_affected
                            
                            max_modstamp_in_batch = None
                            last_record_id_in_batch = None
                            
                            for batch_item in db_update_batch:
                                current_modstamp_dt = batch_item[4] # SystemModstamp (datetime object) is at index 4
                                current_record_id = batch_item[0] # Id is at index 0

                                if max_modstamp_in_batch is None or current_modstamp_dt > max_modstamp_in_batch:
                                    max_modstamp_in_batch = current_modstamp_dt
                                    last_record_id_in_batch = current_record_id

                            if max_modstamp_in_batch:
                                max_modstamp_sf_format = max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                                _update_sync_state(cursor, cnxn, 'ContentDocumentLinkSync', 
                                                     last_record_id_in_batch, 
                                                     max_modstamp_sf_format)

                                for r in processed_records:
                                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                                        r['SqlUpdateStatus'] = 'Success (Batched)'
                                        r['LastSystemModstampInBatch'] = max_modstamp_sf_format
                            else:
                                print(f"    WARNING: No valid SystemModstamp found in batch for ContentDocumentLink sync state update.")
                                for r in processed_records:
                                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                                        r['SqlUpdateStatus'] = 'Skipped (No valid timestamp in batch)'
                        else: # rows_affected < 0, indicating DB error
                            for r in processed_records:
                                if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                                    r['SqlUpdateStatus'] = 'Failed (Batched)'
                        db_update_batch = []

                except Exception as e:
                    print(f"    ERROR processing ContentDocumentLink ID: {cdl_id}: {e}")
                    record['ProcessingError'] = str(e)
                    record['SqlUpdateStatus'] = 'Failed (Processing Error)'
            else:
                reason = ""
                if not cdl_id: reason += "No Id. "
                if not content_document_id: reason += "No ContentDocumentId. "
                # Changed variable name in message
                if system_modstamp_ms is None: reason += "No SystemModstamp. " 
                record['ProcessingError'] = f"Skipped: {reason.strip()}"
                record['SqlUpdateStatus'] = 'Not Attempted (Missing Data)'
            
            processed_records.append(record)

        # --- Execute any remaining batch updates after loop ---
        if db_update_batch:
            print(f"    Executing final ContentDocumentLink batch MERGE for {len(db_update_batch)} records...")
            rows_affected = _execute_cdl_db_batch(cursor, cnxn, db_update_batch)
            
            if rows_affected >= 0:
                sql_batch_update_count += rows_affected
                
                max_modstamp_in_batch = None
                last_record_id_in_batch = None
                for batch_item in db_update_batch:
                    current_modstamp_dt = batch_item[4]
                    current_record_id = batch_item[0]

                    if max_modstamp_in_batch is None or current_modstamp_dt > max_modstamp_in_batch:
                        max_modstamp_in_batch = current_modstamp_dt
                        last_record_id_in_batch = current_record_id

                if max_modstamp_in_batch:
                    max_modstamp_sf_format = max_modstamp_in_batch.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                    _update_sync_state(cursor, cnxn, 'ContentDocumentLinkSync', 
                                         last_record_id_in_batch, 
                                         max_modstamp_sf_format)
                    for r in processed_records:
                        if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                            r['SqlUpdateStatus'] = 'Success (Batched - Final)'
                            r['LastSystemModstampInBatch'] = max_modstamp_sf_format
                else:
                    print(f"    WARNING: No valid SystemModstamp found in final ContentDocumentLink batch for sync state update.")
                    for r in processed_records:
                        if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                            r['SqlUpdateStatus'] = 'Skipped (No valid timestamp in final batch)'
            else: # rows_affected < 0, indicating DB error
                for r in processed_records:
                    if r.get('SqlUpdateStatus') == 'Pending Batch Update':
                        r['SqlUpdateStatus'] = 'Failed (Batched - Final)'
            
        if not processed_records:
            print(f"No ContentDocumentLink records found since the last sync timestamp ({soql_start_timestamp}), or none processed.")
            return None

        print(f"Summary for ContentDocumentLink sync:")
        print(f"Total ContentDocumentLink records processed: {record_count}")
        print(f"Total SQL DB records merged via batches: {sql_batch_update_count}")

        return processed_records

    except (SalesforceError, ValueError, pyodbc.Error) as e:
        print(f"A critical error occurred during ContentDocumentLink sync: {e}")
        return None
    except Exception as e:
        print(f"An unexpected general error occurred during ContentDocumentLink sync: {e}")
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
    
    # For Azure Blob Storage (only needed for ContentVersion sync):
    # export AZURE_STORAGE_ACCOUNT_NAME="your_storage_account_name"
    # export AZURE_STORAGE_CONTAINER_NAME="your_container_name"
    # export AZURE_STORAGE_ACCOUNT_KEY="your_storage_account_key" # OR AZURE_STORAGE_CONNECTION_STRING (for Blob)
    
    # For Azure SQL Database:
    # export AZURE_SQL_CONNECTION_STRING="DRIVER={ODBC Driver 17 for SQL Server};SERVER=yourserver.database.windows.net;DATABASE=yourdatabase;UID=yourusername;PWD=yourpassword"
    # export AZURE_DB_BATCH_SIZE="5" # Optional, now defaults to 5

    SF_USERNAME = os.environ.get('SF_USERNAME')
    SF_PASSWORD = os.environ.get('SF_PASSWORD')
    SF_SECURITY_TOKEN = os.environ.get('SF_SECURITY_TOKEN')
    
    # Initial timestamp MUST be in Salesforce ISO 8601 format (e.g., '2024-01-01T00:00:00Z')
    # because the SOQL query 'WHERE SystemModstamp > {soql_start_timestamp}' expects it.
    INITIAL_LAST_SYNC_TIMESTAMP = '2024-01-01T00:00:00Z' 
    IS_SANDBOX = False 

    print(f"Starting ContentVersion sync process.")
    
    # Call the ContentVersion sync function
    content_version_results = download_content_versions_and_files_to_azure_blob_and_sql_batched(
        SF_USERNAME, 
        SF_PASSWORD, 
        SF_SECURITY_TOKEN, 
        INITIAL_LAST_SYNC_TIMESTAMP, 
        sandbox=IS_SANDBOX
    )

    if content_version_results is not None:
        print("\nFinal Processed ContentVersion Data (first 5 records):")
        if content_version_results:
            for i, record in enumerate(content_version_results[:5]):
                print(f"Record {i+1}:")
                print(f"    Id: {record.get('Id')}")
                print(f"    Title: {record.get('Title')}")
                print(f"    FileExtension: {record.get('FileExtension')}")
                print(f"    AzureBlobUrl: {record.get('AzureBlobUrl')}")
                print(f"    SqlUpdateStatus: {record.get('SqlUpdateStatus')}")
                print(f"    LastSystemModstampInBatch: {record.get('LastSystemModstampInBatch')}")
                print(f"    DownloadError: {record.get('DownloadError')}")
                print("-" * 20)
        else:
            print("No ContentVersion records to display.")
        print(f"\nTotal ContentVersion records processed by script: {len(content_version_results) if content_version_results else 0}")
    else:
        print("ContentVersion sync terminated due to a critical error.")

    # print(f"\n{'='*50}\n") # Separator for clarity

    # print(f"Starting ContentDocumentLink sync process.")
    # # Call the new ContentDocumentLink sync function
    # content_document_link_results = download_content_document_links_to_sql_batched(
    #     SF_USERNAME,
    #     SF_PASSWORD,
    #     SF_SECURITY_TOKEN,
    #     INITIAL_LAST_SYNC_TIMESTAMP,
    #     sandbox=IS_SANDBOX
    # )

    # if content_document_link_results is not None:
    #     print("\nFinal Processed ContentDocumentLink Data (first 5 records):")
    #     if content_document_link_results:
    #         for i, record in enumerate(content_document_link_results[:5]):
    #             print(f"Record {i+1}:")
    #             print(f"    Id: {record.get('Id')}")
    #             print(f"    LinkedEntityId: {record.get('LinkedEntityId')}")
    #             print(f"    ContentDocumentId: {record.get('ContentDocumentId')}")
    #             print(f"    IsDeleted: {record.get('IsDeleted')}")
    #             print(f"    SqlUpdateStatus: {record.get('SqlUpdateStatus')}")
    #             print(f"    LastSystemModstampInBatch: {record.get('LastSystemModstampInBatch')}")
    #             print(f"    ProcessingError: {record.get('ProcessingError')}")
    #             print("-" * 20)
    #     else:
    #         print("No ContentDocumentLink records to display.")
    #     print(f"\nTotal ContentDocumentLink records processed by script: {len(content_document_link_results) if content_document_link_results else 0}")
    # else:
    #     print("ContentDocumentLink sync terminated due to a critical error.")