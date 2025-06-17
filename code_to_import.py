import logging

def helper_code() -> None:
    logging.info("Helper code function successfully executed. AB")
    
import csv
from io import StringIO
import os
import requests
from simple_salesforce import Salesforce, SalesforceError
import pandas as pd # Optional: for easier data handling

def download_content_versions_and_files_bulk_api(
    username, password, security_token, last_sync_timestamp, 
    download_directory="downloaded_files", sandbox=False
):
    """
    Downloads ContentVersion objects from Salesforce using Bulk API 2.0
    with a SystemModstamp greater than the provided timestamp,
    and then downloads the associated files using their VersionDataUrl.

    Args:
        username (str): Salesforce username.
        password (str): Salesforce password.
        security_token (str): Salesforce security token.
        last_sync_timestamp (str): Timestamp string (e.g., '2024-01-01T00:00:00Z')
                                   to filter records by SystemModstamp greater than this.
        download_directory (str, optional): The directory to save downloaded files.
                                            Defaults to "downloaded_files".
        sandbox (bool, optional): Set to True if connecting to a sandbox environment.
                                  Defaults to False (production).

    Returns:
        pandas.DataFrame or list: A Pandas DataFrame containing the downloaded records
                                  (including the path to downloaded files if successful),
                                  if pandas is installed, otherwise a list of dictionaries.
                                  Returns None if an error occurs.
    """
    sf = None # Initialize sf to None
    try:
        # 1. Authenticate with Salesforce
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            domain='test' if sandbox else 'login'
        )
        print(f"Successfully connected to Salesforce. API version: {sf.api_version}")

        # Ensure download directory exists
        os.makedirs(download_directory, exist_ok=True)

        # 2. Construct the SOQL query
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

        # 3. Execute the Bulk API query (Bulk 2.0)
        # job_result is an iterator that yields records or batches of records
        job_result = sf.bulk.ContentVersion.query(soql_query)

        # Get the session ID for file downloads
        session_id = sf.session_id

        # List to store records *after* processing (including download attempts)
        processed_records = []
        download_count = 0
        record_count = 0

        print("Starting file downloads...")
        # 4. Iterate directly through job_result and download files
        for record in job_result: # Iterate directly over the records yielded by job_result
            record_count += 1
            version_data_url = record.get('VersionDataUrl')
            file_extension = record.get('FileExtension')
            title = record.get('Title')
            content_version_id = record.get('Id')

            if version_data_url:
                # Using VersionDataUrl directly as the full URL as per previous request
                full_download_url = version_data_url 
                
                # Use a cleaner filename if possible, otherwise fall back to ID
                # Sanitize filename for common OS compatibility issues
                safe_title = "".join([c for c in (title or content_version_id) if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
                filename = f"{safe_title}"
                if file_extension:
                    filename = f"{filename}.{file_extension}"
                else:
                    filename = f"{filename}.bin" # Generic binary extension

                file_path = os.path.join(download_directory, filename)

                try:
                    headers = {
                        'Authorization': f'Bearer {session_id}'
                    }
                    
                    response = requests.get(full_download_url, headers=headers, stream=True)
                    response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"  Downloaded: {filename} (ID: {content_version_id}) to {file_path}")
                    record['DownloadedFilePath'] = file_path # Add path to the record
                    download_count += 1
                except requests.exceptions.RequestException as req_e:
                    print(f"  Error downloading {filename} (ID: {content_version_id}) from {full_download_url}: {req_e}")
                    record['DownloadError'] = str(req_e)
                except Exception as e:
                    print(f"  An unexpected error occurred during download of {filename} (ID: {content_version_id}): {e}")
                    record['DownloadError'] = str(e)
            else:
                # print(f"  No VersionDataUrl found for ContentVersion ID: {content_version_id}") # Uncomment for verbose
                record['DownloadError'] = "No VersionDataUrl"
            
            # Add the processed record to the list
            processed_records.append(record)

        if not processed_records:
            print("No ContentVersion records found since the last sync timestamp, or none processed.")
            return None

        print(f"Total ContentVersion metadata records processed: {record_count}")
        print(f"Total files downloaded: {download_count}")

        # 5. Process the results (optional: convert to DataFrame)
        if 'pd' in globals(): # Check if pandas was imported
            df = pd.DataFrame(processed_records)
            return df
        else:
            return processed_records

    except SalesforceError as e:
        print(f"Salesforce API Error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
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
    SF_USERNAME = os.environ.get('SF_USERNAME', 'danny.tekumalla@lendzfinancial.com')
    SF_PASSWORD = os.environ.get('SF_PASSWORD', 'KKr@Aug06!996')
    SF_SECURITY_TOKEN = os.environ.get('SF_SECURITY_TOKEN', 'yIMQuZzUFNz6NFOQKJA1ua0f')
    
    LAST_SYNC_TIMESTAMP = '2025-04-30T00:00:00Z' 
    
    IS_SANDBOX = False 
    
    DOWNLOAD_DIR = "downloaded_content_files" 

    print(f"Attempting to download ContentVersion records and files modified after: {LAST_SYNC_TIMESTAMP}")
    
    content_versions_with_files = download_content_versions_and_files_bulk_api(
        SF_USERNAME, 
        SF_PASSWORD, 
        SF_SECURITY_TOKEN, 
        LAST_SYNC_TIMESTAMP, 
        download_directory=DOWNLOAD_DIR,
        sandbox=IS_SANDBOX
    )

    if content_versions_with_files is not None:
        if isinstance(content_versions_with_files, pd.DataFrame):
            print("\nDownloaded ContentVersion Data with File Paths (first 5 rows):")
            print(content_versions_with_files[['Id', 'Title', 'FileExtension', 'DownloadedFilePath', 'DownloadError']].head())
            print(f"\nTotal records processed: {len(content_versions_with_files)}")
        else:
            print("\nDownloaded ContentVersion Data (first record with file info):")
            print(content_versions_with_files[0] if content_versions_with_files else "No records to display.")
            print(f"\nTotal records processed: {len(content_versions_with_files)}")
    else:
        print("Failed to download ContentVersion records or files.")