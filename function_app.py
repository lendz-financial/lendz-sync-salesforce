import azure.functions as func
from code_to_import import helper_code,download_content_document_links_to_sql_batched,download_content_versions_and_files_to_azure_blob_and_sql_batched
import logging
import os

#app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app = func.FunctionApp()

# @app.function_name("SalesforceSyncFunction1")
# @app.route(route="salesforcesyncfunction1")
# def salesforce_sync_function_1(req: func.HttpRequest) -> func.HttpResponse:
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger(__name__)
#     logging.info("Your example function was successfully triggered.")

#     # do something with the req argument
#     events = req.get_json()
#     logger.info(f"Request body:\n{events}")

#     # execute imported functions
#     helper_code()

#     # put your own code here ...


#     # finish execution
#     logger.info("Finished execution.")
#     return func.HttpResponse()


@app.function_name("sf_sync_contentversion")
@app.timer_trigger(schedule="0 10 * * * *", arg_name="sf_cv_timer", run_on_startup=False, use_monitor=False)
def lendz_sync_salesforce_contentversion(myTimer: func.TimerRequest) -> None:
    logging.info('====== sync_salesforce_contentversion function executed.')
    my_connection_string = os.getenv('SQL_CONNECTION_STRING')
    if not my_connection_string:
        raise ValueError("SQL_CONNECTION_STRING environment variable not set. Please set it before running the script.")
    # Call the new orchestrating function to fetch data from LoanPASS API and process it
    helper_code()

@app.function_name("sf_sync_contentdocumentlink")    
@app.timer_trigger(schedule="0 25 * * * *", arg_name="sf_cdl_timer", run_on_startup=False, use_monitor=False)
def lendz_sync_salesforce_contentdocumentlink(myTimer: func.TimerRequest) -> None:
    logging.info('====== sync_salesforce_contentdocumentlink function executed.')
    SF_USERNAME = os.environ.get('SF_USERNAME')
    SF_PASSWORD = os.environ.get('SF_PASSWORD')
    SF_SECURITY_TOKEN = os.environ.get('SF_SECURITY_TOKEN')
    
    # Initial timestamp MUST be in Salesforce ISO 8601 format (e.g., '2024-01-01T00:00:00Z')
    # because the SOQL query 'WHERE SystemModstamp > {soql_start_timestamp}' expects it.
    INITIAL_LAST_SYNC_TIMESTAMP = '2024-01-01T00:00:00Z' 
    IS_SANDBOX = False     
    print(f"Starting ContentDocumentLink sync process.")
    # Call the new ContentDocumentLink sync function
    content_document_link_results = download_content_document_links_to_sql_batched(
        SF_USERNAME,
        SF_PASSWORD,
        SF_SECURITY_TOKEN,
        INITIAL_LAST_SYNC_TIMESTAMP,
        sandbox=IS_SANDBOX
    )

    if content_document_link_results is not None:
        print("\nFinal Processed ContentDocumentLink Data (first 5 records):")
        if content_document_link_results:
            for i, record in enumerate(content_document_link_results[:5]):
                print(f"Record {i+1}:")
                print(f"    Id: {record.get('Id')}")
                print(f"    LinkedEntityId: {record.get('LinkedEntityId')}")
                print(f"    ContentDocumentId: {record.get('ContentDocumentId')}")
                print(f"    IsDeleted: {record.get('IsDeleted')}")
                print(f"    SqlUpdateStatus: {record.get('SqlUpdateStatus')}")
                print(f"    LastSystemModstampInBatch: {record.get('LastSystemModstampInBatch')}")
                print(f"    ProcessingError: {record.get('ProcessingError')}")
                print("-" * 20)
        else:
            print("No ContentDocumentLink records to display.")
        print(f"\nTotal ContentDocumentLink records processed by script: {len(content_document_link_results) if content_document_link_results else 0}")
    else:
        print("ContentDocumentLink sync terminated due to a critical error.") 