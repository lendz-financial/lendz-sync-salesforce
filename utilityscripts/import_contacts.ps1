# PowerShell Script to import CSV data into a SQL Server table
#
# This script reads a CSV file, performs necessary data type conversions
# based on the SQL Server table schema, and then uses SqlBulkCopy
# to efficiently insert the data into the specified SQL Server table.

<#
.SYNOPSIS
    Imports data from a CSV file into a SQL Server table.

.DESCRIPTION
    This script takes a CSV file path, SQL Server connection details,
    the target table name, and an optional batch size as input. It reads the CSV,
    converts data types (nvarchar, bit, float, datetime2) as appropriate for
    the SQL table, and uses SqlBulkCopy for high-performance data insertion,
    batching the inserts for better performance with large datasets.
    Empty strings in the CSV are treated as NULL for nullable columns
    of type bit, float, and datetime2.

.PARAMETER CsvFilePath
    The full path to the CSV file to be imported.

.PARAMETER ServerName
    The name of the SQL Server instance (e.g., 'localhost', 'SERVER\INSTANCE').

.PARAMETER DatabaseName
    The name of the database where the target table resides.

.PARAMETER TableName
    The full name of the target table (e.g., 'dbo.Contact').

.PARAMETER BatchSize
    The number of rows to send in each batch operation to SQL Server.
    Default is 500.

.EXAMPLE
    .\Import-ContactCsv.ps1 -CsvFilePath "C:\Data\contacts.csv" `
        -ServerName "YourSqlServerInstance" -DatabaseName "YourDatabase" `
        -TableName "dbo.Contact" -BatchSize 1000

.NOTES
    - Ensure the CSV file has a header row that matches the SQL table column names.
    - SQL Server Management Objects (SMO) are not strictly required for SqlBulkCopy,
      but the .NET Framework Data Provider for SQL Server is. This is usually
      available by default in Windows.
    - For large CSVs, ensure your PowerShell memory limits are sufficient.
    - If you encounter character encoding issues, try specifying the -Encoding
      parameter for Import-Csv (e.g., -Encoding UTF8).
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$CsvFilePath,

    [Parameter(Mandatory=$true)]
    [string]$ServerName,

    [Parameter(Mandatory=$true)]
    [string]$DatabaseName,

    [Parameter(Mandatory=$true)]
    [string]$TableName,

    [int]$BatchSize = 500 # New parameter for configurable batch size
)

# --- SQL Server Connection Details ---
# Using SQL Server Authentication as requested by the user
$connectionString = "Server=lendz.database.windows.net;Database=Lexi;User ID=lexi;Password=H3n4y*_D@;"

Write-Host "Starting CSV import to SQL Server..."
Write-Host "CSV File: $CsvFilePath"
Write-Host "SQL Server: lendz.database.windows.net"
Write-Host "Database: Lexi"
Write-Host "Table: $TableName"
Write-Host "Batch Size: $BatchSize"

try {
    # --- Import CSV Data ---
    Write-Host "Importing CSV file..."
    $csvData = Import-Csv -Path $CsvFilePath -ErrorAction Stop

    if (-not $csvData) {
        Write-Warning "The CSV file '$CsvFilePath' is empty or could not be read."
        exit 1
    }
    Write-Host "Successfully loaded $($csvData.Count) records from CSV."

    # --- Prepare DataTable for SqlBulkCopy ---
    $dataTable = New-Object System.Data.DataTable
    $columnMappings = @{} # Dictionary to store column names and their target types for conversion

    # Define columns and their types based on your SQL table schema
    # IMPORTANT: Ensure these match your SQL table's column names and types exactly.
    # Add all columns from your SQL table definition here.

    $dataTable.Columns.Add("Id", [System.String]) | Out-Null
    $columnMappings["Id"] = [System.String]

    $dataTable.Columns.Add("IsDeleted", [System.Boolean]) | Out-Null
    $columnMappings["IsDeleted"] = [System.Boolean]

    $dataTable.Columns.Add("MasterRecordId", [System.String]) | Out-Null
    $columnMappings["MasterRecordId"] = [System.String]

    $dataTable.Columns.Add("AccountId", [System.String]) | Out-Null
    $columnMappings["AccountId"] = [System.String]

    $dataTable.Columns.Add("LastName", [System.String]) | Out-Null
    $columnMappings["LastName"] = [System.String]

    $dataTable.Columns.Add("FirstName", [System.String]) | Out-Null
    $columnMappings["FirstName"] = [System.String]

    $dataTable.Columns.Add("Salutation", [System.String]) | Out-Null
    $columnMappings["Salutation"] = [System.String]

    $dataTable.Columns.Add("MiddleName", [System.String]) | Out-Null
    $columnMappings["MiddleName"] = [System.String]

    $dataTable.Columns.Add("Suffix", [System.String]) | Out-Null
    $columnMappings["Suffix"] = [System.String]

    $dataTable.Columns.Add("Name", [System.String]) | Out-Null
    $columnMappings["Name"] = [System.String] # This column is in SQL but not in your example CSV header. If it's derivable, handle it. Assuming it's in CSV too.

    # Other Address Fields
    $dataTable.Columns.Add("OtherStreet", [System.String]) | Out-Null
    $columnMappings["OtherStreet"] = [System.String]
    $dataTable.Columns.Add("OtherCity", [System.String]) | Out-Null
    $columnMappings["OtherCity"] = [System.String]
    $dataTable.Columns.Add("OtherState", [System.String]) | Out-Null
    $columnMappings["OtherState"] = [System.String]
    $dataTable.Columns.Add("OtherPostalCode", [System.String]) | Out-Null
    $columnMappings["OtherPostalCode"] = [System.String]
    $dataTable.Columns.Add("OtherCountry", [System.String]) | Out-Null
    $columnMappings["OtherCountry"] = [System.String]
    $dataTable.Columns.Add("OtherStateCode", [System.String]) | Out-Null
    $columnMappings["OtherStateCode"] = [System.String]
    $dataTable.Columns.Add("OtherCountryCode", [System.String]) | Out-Null
    $columnMappings["OtherCountryCode"] = [System.String]
    $dataTable.Columns.Add("OtherLatitude", [System.Double]) | Out-Null
    $columnMappings["OtherLatitude"] = [System.Double]
    $dataTable.Columns.Add("OtherLongitude", [System.Double]) | Out-Null
    $columnMappings["OtherLongitude"] = [System.Double]
    $dataTable.Columns.Add("OtherGeocodeAccuracy", [System.String]) | Out-Null
    $columnMappings["OtherGeocodeAccuracy"] = [System.String]
    $dataTable.Columns.Add("OtherAddress", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["OtherAddress"] = [System.String]

    # Mailing Address Fields
    $dataTable.Columns.Add("MailingStreet", [System.String]) | Out-Null
    $columnMappings["MailingStreet"] = [System.String]
    $dataTable.Columns.Add("MailingCity", [System.String]) | Out-Null
    $columnMappings["MailingCity"] = [System.String]
    $dataTable.Columns.Add("MailingState", [System.String]) | Out-Null
    $columnMappings["MailingState"] = [System.String]
    $dataTable.Columns.Add("MailingPostalCode", [System.String]) | Out-Null
    $columnMappings["MailingPostalCode"] = [System.String]
    $dataTable.Columns.Add("MailingCountry", [System.String]) | Out-Null
    $columnMappings["MailingCountry"] = [System.String]
    $dataTable.Columns.Add("MailingStateCode", [System.String]) | Out-Null
    $columnMappings["MailingStateCode"] = [System.String]
    $dataTable.Columns.Add("MailingCountryCode", [System.String]) | Out-Null
    $columnMappings["MailingCountryCode"] = [System.String]
    $dataTable.Columns.Add("MailingLatitude", [System.Double]) | Out-Null
    $columnMappings["MailingLatitude"] = [System.Double]
    $dataTable.Columns.Add("MailingLongitude", [System.Double]) | Out-Null
    $columnMappings["MailingLongitude"] = [System.Double]
    $dataTable.Columns.Add("MailingGeocodeAccuracy", [System.String]) | Out-Null
    $columnMappings["MailingGeocodeAccuracy"] = [System.String]
    $dataTable.Columns.Add("MailingAddress", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["MailingAddress"] = [System.String]

    # Phone/Email Fields
    $dataTable.Columns.Add("Phone", [System.String]) | Out-Null
    $columnMappings["Phone"] = [System.String]
    $dataTable.Columns.Add("Fax", [System.String]) | Out-Null
    $columnMappings["Fax"] = [System.String]
    $dataTable.Columns.Add("MobilePhone", [System.String]) | Out-Null
    $columnMappings["MobilePhone"] = [System.String]
    $dataTable.Columns.Add("HomePhone", [System.String]) | Out-Null
    $columnMappings["HomePhone"] = [System.String]
    $dataTable.Columns.Add("OtherPhone", [System.String]) | Out-Null
    $columnMappings["OtherPhone"] = [System.String]
    $dataTable.Columns.Add("AssistantPhone", [System.String]) | Out-Null
    $columnMappings["AssistantPhone"] = [System.String]
    $dataTable.Columns.Add("ReportsToId", [System.String]) | Out-Null
    $columnMappings["ReportsToId"] = [System.String]
    $dataTable.Columns.Add("Email", [System.String]) | Out-Null
    $columnMappings["Email"] = [System.String]

    # Other Personal/Job Details
    $dataTable.Columns.Add("Title", [System.String]) | Out-Null
    $columnMappings["Title"] = [System.String]
    $dataTable.Columns.Add("Department", [System.String]) | Out-Null
    $columnMappings["Department"] = [System.String]
    $dataTable.Columns.Add("AssistantName", [System.String]) | Out-Null
    $columnMappings["AssistantName"] = [System.String]
    $dataTable.Columns.Add("LeadSource", [System.String]) | Out-Null
    $columnMappings["LeadSource"] = [System.String]
    $dataTable.Columns.Add("Birthdate", [System.DateTime]) | Out-Null
    $columnMappings["Birthdate"] = [System.DateTime]
    $dataTable.Columns.Add("Description", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["Description"] = [System.String]
    $dataTable.Columns.Add("OwnerId", [System.String]) | Out-Null
    $columnMappings["OwnerId"] = [System.String]
    $dataTable.Columns.Add("HasOptedOutOfEmail", [System.Boolean]) | Out-Null
    $columnMappings["HasOptedOutOfEmail"] = [System.Boolean]
    $dataTable.Columns.Add("HasOptedOutOfFax", [System.Boolean]) | Out-Null
    $columnMappings["HasOptedOutOfFax"] = [System.Boolean]
    $dataTable.Columns.Add("DoNotCall", [System.Boolean]) | Out-Null
    $columnMappings["DoNotCall"] = [System.Boolean]

    # Salesforce/Activity Tracking Fields
    $dataTable.Columns.Add("ActionCadenceId", [System.String]) | Out-Null
    $columnMappings["ActionCadenceId"] = [System.String]
    $dataTable.Columns.Add("ActionCadenceAssigneeId", [System.String]) | Out-Null
    $columnMappings["ActionCadenceAssigneeId"] = [System.String]
    $dataTable.Columns.Add("ActionCadenceState", [System.String]) | Out-Null
    $columnMappings["ActionCadenceState"] = [System.String]
    $dataTable.Columns.Add("ScheduledResumeDateTime", [System.DateTime]) | Out-Null
    $columnMappings["ScheduledResumeDateTime"] = [System.DateTime]
    $dataTable.Columns.Add("ActiveTrackerCount", [System.Int32]) | Out-Null # int
    $columnMappings["ActiveTrackerCount"] = [System.Int32]
    $dataTable.Columns.Add("CreatedDate", [System.DateTime]) | Out-Null
    $columnMappings["CreatedDate"] = [System.DateTime]
    $dataTable.Columns.Add("CreatedById", [System.String]) | Out-Null
    $columnMappings["CreatedById"] = [System.String]
    $dataTable.Columns.Add("LastModifiedDate", [System.DateTime]) | Out-Null
    $columnMappings["LastModifiedDate"] = [System.DateTime]
    $dataTable.Columns.Add("LastModifiedById", [System.String]) | Out-Null
    $columnMappings["LastModifiedById"] = [System.String]
    $dataTable.Columns.Add("SystemModstamp", [System.DateTime]) | Out-Null
    $columnMappings["SystemModstamp"] = [System.DateTime]
    $dataTable.Columns.Add("LastActivityDate", [System.DateTime]) | Out-Null
    $columnMappings["LastActivityDate"] = [System.DateTime]
    $dataTable.Columns.Add("LastCURequestDate", [System.DateTime]) | Out-Null
    $columnMappings["LastCURequestDate"] = [System.DateTime]
    $dataTable.Columns.Add("LastCUUpdateDate", [System.DateTime]) | Out-Null
    $columnMappings["LastCUUpdateDate"] = [System.DateTime]
    $dataTable.Columns.Add("LastViewedDate", [System.DateTime]) | Out-Null
    $columnMappings["LastViewedDate"] = [System.DateTime]
    $dataTable.Columns.Add("LastReferencedDate", [System.DateTime]) | Out-Null
    $columnMappings["LastReferencedDate"] = [System.DateTime]

    $dataTable.Columns.Add("EmailBouncedReason", [System.String]) | Out-Null
    $columnMappings["EmailBouncedReason"] = [System.String]
    $dataTable.Columns.Add("EmailBouncedDate", [System.DateTime]) | Out-Null
    $columnMappings["EmailBouncedDate"] = [System.DateTime]
    $dataTable.Columns.Add("IsEmailBounced", [System.Boolean]) | Out-Null
    $columnMappings["IsEmailBounced"] = [System.Boolean]
    $dataTable.Columns.Add("PhotoUrl", [System.String]) | Out-Null
    $columnMappings["PhotoUrl"] = [System.String]
    $dataTable.Columns.Add("Jigsaw", [System.String]) | Out-Null
    $columnMappings["Jigsaw"] = [System.String]
    $dataTable.Columns.Add("JigsawContactId", [System.String]) | Out-Null
    $columnMappings["JigsawContactId"] = [System.String]

    $dataTable.Columns.Add("FirstCallDateTime", [System.DateTime]) | Out-Null
    $columnMappings["FirstCallDateTime"] = [System.DateTime]
    $dataTable.Columns.Add("FirstEmailDateTime", [System.DateTime]) | Out-Null
    $columnMappings["FirstEmailDateTime"] = [System.DateTime]
    $dataTable.Columns.Add("Pronouns", [System.String]) | Out-Null
    $columnMappings["Pronouns"] = [System.String]
    $dataTable.Columns.Add("GenderIdentity", [System.String]) | Out-Null
    $columnMappings["GenderIdentity"] = [System.String]
    $dataTable.Columns.Add("ActivityMetricId", [System.String]) | Out-Null
    $columnMappings["ActivityMetricId"] = [System.String]
    $dataTable.Columns.Add("ContactSource", [System.String]) | Out-Null
    $columnMappings["ContactSource"] = [System.String]
    $dataTable.Columns.Add("TitleType", [System.String]) | Out-Null
    $columnMappings["TitleType"] = [System.String]
    $dataTable.Columns.Add("DepartmentGroup", [System.String]) | Out-Null
    $columnMappings["DepartmentGroup"] = [System.String]
    $dataTable.Columns.Add("BuyerAttributes", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["BuyerAttributes"] = [System.String]

    # pi__ (Pardot/Marketing Automation) Custom Fields
    $dataTable.Columns.Add("pi__Needs_Score_Synced__c", [System.Boolean]) | Out-Null
    $columnMappings["pi__Needs_Score_Synced__c"] = [System.Boolean]
    $dataTable.Columns.Add("pi__Pardot_Last_Scored_At__c", [System.DateTime]) | Out-Null
    $columnMappings["pi__Pardot_Last_Scored_At__c"] = [System.DateTime]
    $dataTable.Columns.Add("pi__campaign__c", [System.String]) | Out-Null
    $columnMappings["pi__campaign__c"] = [System.String]
    $dataTable.Columns.Add("pi__comments__c", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["pi__comments__c"] = [System.String]
    $dataTable.Columns.Add("pi__conversion_date__c", [System.DateTime]) | Out-Null
    $columnMappings["pi__conversion_date__c"] = [System.DateTime]
    $dataTable.Columns.Add("pi__conversion_object_name__c", [System.String]) | Out-Null
    $columnMappings["pi__conversion_object_name__c"] = [System.String]
    $dataTable.Columns.Add("pi__conversion_object_type__c", [System.String]) | Out-Null
    $columnMappings["pi__conversion_object_type__c"] = [System.String]
    $dataTable.Columns.Add("pi__created_date__c", [System.DateTime]) | Out-Null
    $columnMappings["pi__created_date__c"] = [System.DateTime]
    $dataTable.Columns.Add("pi__first_activity__c", [System.DateTime]) | Out-Null
    $columnMappings["pi__first_activity__c"] = [System.DateTime]
    $dataTable.Columns.Add("pi__first_search_term__c", [System.String]) | Out-Null
    $columnMappings["pi__first_search_term__c"] = [System.String]
    $dataTable.Columns.Add("pi__first_search_type__c", [System.String]) | Out-Null
    $columnMappings["pi__first_search_type__c"] = [System.String]
    $dataTable.Columns.Add("pi__first_touch_url__c", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["pi__first_touch_url__c"] = [System.String]
    $dataTable.Columns.Add("pi__grade__c", [System.String]) | Out-Null
    $columnMappings["pi__grade__c"] = [System.String]
    $dataTable.Columns.Add("pi__last_activity__c", [System.DateTime]) | Out-Null
    $columnMappings["pi__last_activity__c"] = [System.DateTime]
    $dataTable.Columns.Add("pi__notes__c", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["pi__notes__c"] = [System.String]
    $dataTable.Columns.Add("pi__pardot_hard_bounced__c", [System.Boolean]) | Out-Null
    $columnMappings["pi__pardot_hard_bounced__c"] = [System.Boolean]
    $dataTable.Columns.Add("pi__score__c", [System.Double]) | Out-Null
    $columnMappings["pi__score__c"] = [System.Double]
    $dataTable.Columns.Add("pi__url__c", [System.String]) | Out-Null
    $columnMappings["pi__url__c"] = [System.String]
    $dataTable.Columns.Add("pi__utm_campaign__c", [System.String]) | Out-Null
    $columnMappings["pi__utm_campaign__c"] = [System.String]
    $dataTable.Columns.Add("pi__utm_content__c", [System.String]) | Out-Null
    $columnMappings["pi__utm_content__c"] = [System.String]
    $dataTable.Columns.Add("pi__utm_medium__c", [System.String]) | Out-Null
    $columnMappings["pi__utm_medium__c"] = [System.String]
    $dataTable.Columns.Add("pi__utm_source__c", [System.String]) | Out-Null
    $columnMappings["pi__utm_source__c"] = [System.String]
    $dataTable.Columns.Add("pi__utm_term__c", [System.String]) | Out-Null
    $columnMappings["pi__utm_term__c"] = [System.String]

    # Loan-related Custom Fields
    $dataTable.Columns.Add("Last_Funded__c", [System.DateTime]) | Out-Null
    $columnMappings["Last_Funded__c"] = [System.DateTime]
    $dataTable.Columns.Add("Last_Reassign_Date__c", [System.DateTime]) | Out-Null
    $columnMappings["Last_Reassign_Date__c"] = [System.DateTime]
    $dataTable.Columns.Add("Last_Refresh_Date__c", [System.DateTime]) | Out-Null
    $columnMappings["Last_Refresh_Date__c"] = [System.DateTime]
    $dataTable.Columns.Add("Last_Submission__c", [System.DateTime]) | Out-Null
    $columnMappings["Last_Submission__c"] = [System.DateTime]
    $dataTable.Columns.Add("Loan_Submissions_All_Time__c", [System.Double]) | Out-Null
    $columnMappings["Loan_Submissions_All_Time__c"] = [System.Double]
    $dataTable.Columns.Add("NMLS_ID__c", [System.String]) | Out-Null
    $columnMappings["NMLS_ID__c"] = [System.String]
    $dataTable.Columns.Add("Ownership_Lock_Date__c", [System.DateTime]) | Out-Null
    $columnMappings["Ownership_Lock_Date__c"] = [System.DateTime]
    $dataTable.Columns.Add("Submissions_Last_30__c", [System.Double]) | Out-Null
    $columnMappings["Submissions_Last_30__c"] = [System.Double]
    $dataTable.Columns.Add("Type__c", [System.String]) | Out-Null # nvarchar(1300)
    $columnMappings["Type__c"] = [System.String]

    # nvb_Email_Fields
    $dataTable.Columns.Add("nvb_Email_Flags__c", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["nvb_Email_Flags__c"] = [System.String]
    $dataTable.Columns.Add("nvb_Email_Result__c", [System.String]) | Out-Null
    $columnMappings["nvb_Email_Result__c"] = [System.String]
    $dataTable.Columns.Add("nvb_Email_Status__c", [System.String]) | Out-Null
    $columnMappings["nvb_Email_Status__c"] = [System.String]
    $dataTable.Columns.Add("nvb_Last_Email_Check__c", [System.DateTime]) | Out-Null
    $columnMappings["nvb_Last_Email_Check__c"] = [System.DateTime]

    $dataTable.Columns.Add("Ownership_Status__c", [System.String]) | Out-Null # nvarchar(1300)
    $columnMappings["Ownership_Status__c"] = [System.String]
    $dataTable.Columns.Add("nvb_Email_Check__c", [System.String]) | Out-Null # nvarchar(1300)
    $columnMappings["nvb_Email_Check__c"] = [System.String]
    $dataTable.Columns.Add("Broker_Approved_Date__c", [System.DateTime]) | Out-Null
    $columnMappings["Broker_Approved_Date__c"] = [System.DateTime]
    $dataTable.Columns.Add("Do_Not_Email__c", [System.Boolean]) | Out-Null
    $columnMappings["Do_Not_Email__c"] = [System.Boolean]

    # Modex Fields
    $dataTable.Columns.Add("Modex_12_Months_Average_Monthly__c", [System.Double]) | Out-Null
    $columnMappings["Modex_12_Months_Average_Monthly__c"] = [System.Double]
    $dataTable.Columns.Add("Modex_12_Months_Average__c", [System.Double]) | Out-Null
    $columnMappings["Modex_12_Months_Average__c"] = [System.Double]
    $dataTable.Columns.Add("Modex_12_Months_Count__c", [System.Double]) | Out-Null
    $columnMappings["Modex_12_Months_Count__c"] = [System.Double]
    $dataTable.Columns.Add("Modex_12_Months_Sum__c", [System.Double]) | Out-Null
    $columnMappings["Modex_12_Months_Sum__c"] = [System.Double]
    $dataTable.Columns.Add("Modex_Branch_NMLS_Id__c", [System.String]) | Out-Null
    $columnMappings["Modex_Branch_NMLS_Id__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Company_NMLS_Id__c", [System.String]) | Out-Null
    $columnMappings["Modex_Company_NMLS_Id__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Company_Name__c", [System.String]) | Out-Null
    $columnMappings["Modex_Company_Name__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Company_Website__c", [System.String]) | Out-Null
    $columnMappings["Modex_Company_Website__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Current_Job__c", [System.String]) | Out-Null
    $columnMappings["Modex_Current_Job__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Employer_Name__c", [System.String]) | Out-Null
    $columnMappings["Modex_Employer_Name__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Facebook__c", [System.String]) | Out-Null
    $columnMappings["Modex_Facebook__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Financial_Services_History__c", [System.String]) | Out-Null
    $columnMappings["Modex_Financial_Services_History__c"] = [System.String]
    $dataTable.Columns.Add("Modex_ID__c", [System.String]) | Out-Null
    $columnMappings["Modex_ID__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Jobs_within_10_years__c", [System.String]) | Out-Null
    $columnMappings["Modex_Jobs_within_10_years__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Linkedin__c", [System.String]) | Out-Null
    $columnMappings["Modex_Linkedin__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Office_Phone__c", [System.String]) | Out-Null
    $columnMappings["Modex_Office_Phone__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Other_Email__c", [System.String]) | Out-Null
    $columnMappings["Modex_Other_Email__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Other_Phone__c", [System.String]) | Out-Null
    $columnMappings["Modex_Other_Phone__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Personal_Email__c", [System.String]) | Out-Null
    $columnMappings["Modex_Personal_Email__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Score__c", [System.Double]) | Out-Null
    $columnMappings["Modex_Score__c"] = [System.Double]
    $dataTable.Columns.Add("Modex_Transaction_Summary__c", [System.String]) | Out-Null
    $columnMappings["Modex_Transaction_Summary__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Twitter__c", [System.String]) | Out-Null
    $columnMappings["Modex_Twitter__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Type__c", [System.String]) | Out-Null
    $columnMappings["Modex_Type__c"] = [System.String]
    $dataTable.Columns.Add("Modex_URL__c", [System.String]) | Out-Null
    $columnMappings["Modex_URL__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Work_Email__c", [System.String]) | Out-Null
    $columnMappings["Modex_Work_Email__c"] = [System.String]
    $dataTable.Columns.Add("Modex_Zillow__c", [System.String]) | Out-Null
    $columnMappings["Modex_Zillow__c"] = [System.String]

    $dataTable.Columns.Add("Funded_Last_365__c", [System.Double]) | Out-Null
    $columnMappings["Funded_Last_365__c"] = [System.Double]
    $dataTable.Columns.Add("pi_Weekly_Greeting__c", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["pi_Weekly_Greeting__c"] = [System.String]
    $dataTable.Columns.Add("Capitalization_Issue__c", [System.Boolean]) | Out-Null
    $columnMappings["Capitalization_Issue__c"] = [System.Boolean]
    $dataTable.Columns.Add("Phone_Check__c", [System.String]) | Out-Null # nvarchar(1300)
    $columnMappings["Phone_Check__c"] = [System.String]
    $dataTable.Columns.Add("Primary_Campaign__c", [System.String]) | Out-Null
    $columnMappings["Primary_Campaign__c"] = [System.String]
    $dataTable.Columns.Add("Connects__c", [System.Double]) | Out-Null
    $columnMappings["Connects__c"] = [System.Double]
    $dataTable.Columns.Add("Last_Connect__c", [System.DateTime]) | Out-Null
    $columnMappings["Last_Connect__c"] = [System.DateTime]

    # Non_QM Fields
    $dataTable.Columns.Add("Non_QM_Avg_Loan_Last_12__c", [System.Double]) | Out-Null
    $columnMappings["Non_QM_Avg_Loan_Last_12__c"] = [System.Double]
    $dataTable.Columns.Add("Non_QM_Percent_of_Total__c", [System.Double]) | Out-Null
    $columnMappings["Non_QM_Percent_of_Total__c"] = [System.Double]
    $dataTable.Columns.Add("Non_QM_Units_Last_12__c", [System.Double]) | Out-Null
    $columnMappings["Non_QM_Units_Last_12__c"] = [System.Double]
    $dataTable.Columns.Add("Non_QM_Volume_Last_12__c", [System.Double]) | Out-Null
    $columnMappings["Non_QM_Volume_Last_12__c"] = [System.Double]

    $dataTable.Columns.Add("Lexi_s_Thoughts__c", [System.String]) | Out-Null # nvarchar(max)
    $columnMappings["Lexi_s_Thoughts__c"] = [System.String]
    $dataTable.Columns.Add("Lexi_Score_Number__c", [System.Double]) | Out-Null
    $columnMappings["Lexi_Score_Number__c"] = [System.Double]
    $dataTable.Columns.Add("Case_Safe_ID__c", [System.String]) | Out-Null # nvarchar(1300)
    $columnMappings["Case_Safe_ID__c"] = [System.String]
    $dataTable.Columns.Add("Submissions_Last_365__c", [System.Double]) | Out-Null
    $columnMappings["Submissions_Last_365__c"] = [System.Double]
    $dataTable.Columns.Add("Volume_Rank_LO__c", [System.Double]) | Out-Null
    $columnMappings["Volume_Rank_LO__c"] = [System.Double]
    $dataTable.Columns.Add("Volume_Score_Loan_Originator__c", [System.Double]) | Out-Null
    $columnMappings["Volume_Score_Loan_Originator__c"] = [System.Double]
    $dataTable.Columns.Add("Volume_Last_365__c", [System.Double]) | Out-Null
    $columnMappings["Volume_Last_365__c"] = [System.Double]

    # Dialpad Fields
    $dataTable.Columns.Add("Dialpad__IsCreatedFromDialpad__c", [System.Boolean]) | Out-Null
    $columnMappings["Dialpad__IsCreatedFromDialpad__c"] = [System.Boolean]
    $dataTable.Columns.Add("Dialpad__Powerdialer_Assigned_List__c", [System.String]) | Out-Null
    $columnMappings["Dialpad__Powerdialer_Assigned_List__c"] = [System.String]
    $dataTable.Columns.Add("Dialpad__Powerdialer_Dialed_List__c", [System.String]) | Out-Null
    $columnMappings["Dialpad__Powerdialer_Dialed_List__c"] = [System.String]
    $dataTable.Columns.Add("Dialpad__Powerdialer_Last_Dialed_via__c", [System.String]) | Out-Null
    $columnMappings["Dialpad__Powerdialer_Last_Dialed_via__c"] = [System.String]
    $dataTable.Columns.Add("Dialpad__Timezone__c", [System.String]) | Out-Null
    $columnMappings["Dialpad__Timezone__c"] = [System.String]
    $dataTable.Columns.Add("Dialpad__TotalNumberOfTimesDialed__c", [System.Double]) | Out-Null
    $columnMappings["Dialpad__TotalNumberOfTimesDialed__c"] = [System.Double]

    $dataTable.Columns.Add("Distance__c", [System.Double]) | Out-Null
    $columnMappings["Distance__c"] = [System.Double]
    $dataTable.Columns.Add("Zoho_LO_ID__c", [System.String]) | Out-Null
    $columnMappings["Zoho_LO_ID__c"] = [System.String]
    $dataTable.Columns.Add("Last_Score_Calculation_Date__c", [System.DateTime]) | Out-Null
    $columnMappings["Last_Score_Calculation_Date__c"] = [System.DateTime]
    $dataTable.Columns.Add("LexiScoreVersion__c", [System.String]) | Out-Null
    $columnMappings["LexiScoreVersion__c"] = [System.String]
    $dataTable.Columns.Add("Top_Lender__c", [System.String]) | Out-Null
    $columnMappings["Top_Lender__c"] = [System.String]
    $dataTable.Columns.Add("Top_Lender_Volume__c", [System.Double]) | Out-Null
    $columnMappings["Top_Lender_Volume__c"] = [System.Double]
    $dataTable.Columns.Add("Pull_Through__c", [System.Double]) | Out-Null
    $columnMappings["Pull_Through__c"] = [System.Double]
    $dataTable.Columns.Add("epbLexiScore__c", [System.Double]) | Out-Null
    $columnMappings["epbLexiScore__c"] = [System.Double]
    $dataTable.Columns.Add("Follow_Up_Date__c", [System.DateTime]) | Out-Null
    $columnMappings["Follow_Up_Date__c"] = [System.DateTime]
    $dataTable.Columns.Add("Powerdial__c", [System.Boolean]) | Out-Null
    $columnMappings["Powerdial__c"] = [System.Boolean]
    $dataTable.Columns.Add("Total_Funded__c", [System.Double]) | Out-Null
    $columnMappings["Total_Funded__c"] = [System.Double]
    $dataTable.Columns.Add("Total_Funded_Units__c", [System.Double]) | Out-Null
    $columnMappings["Total_Funded_Units__c"] = [System.Double]
    $dataTable.Columns.Add("Total_Submissions__c", [System.Double]) | Out-Null
    $columnMappings["Total_Submissions__c"] = [System.Double]
    $dataTable.Columns.Add("Knowledge_Rating__c", [System.Double]) | Out-Null
    $columnMappings["Knowledge_Rating__c"] = [System.Double]
    $dataTable.Columns.Add("Preparedness_Rating__c", [System.Double]) | Out-Null
    $columnMappings["Preparedness_Rating__c"] = [System.Double]
    $dataTable.Columns.Add("Responsiveness_Rating__c", [System.Double]) | Out-Null
    $columnMappings["Responsiveness_Rating__c"] = [System.Double]
    $dataTable.Columns.Add("MCAE_Company_Name__c", [System.String]) | Out-Null # nvarchar(1300)
    $columnMappings["MCAE_Company_Name__c"] = [System.String]


    # Date formats to try parsing (most specific first)
    $dateFormat1 = "yyyy-MM-dd HH:mm:ss"
    $dateFormat2 = "yyyy-MM-dd" # For dates without time component

    Write-Host "Populating DataTable from CSV records..."
    $processedRecords = 0
    foreach ($row in $csvData) {
        $dataRow = $dataTable.NewRow()
        $currentRecordErrors = @()

        foreach ($colName in $columnMappings.Keys) {
            $csvValue = $row.$colName
            $targetType = $columnMappings[$colName]

            # Handle empty strings for nullable types
            if ([string]::IsNullOrEmpty($csvValue)) {
                $dataRow.$colName = [System.DBNull]::Value
                continue
            }

            try {
                if ($targetType -eq [System.Boolean]) {
                    # Convert '0', '1', 'TRUE', 'FALSE' to boolean
                    $boolValue = $false
                    if ([bool]::TryParse($csvValue, [ref]$boolValue)) {
                        $dataRow.$colName = $boolValue
                    } elseif ($csvValue -eq '0') {
                        $dataRow.$colName = $false
                    } elseif ($csvValue -eq '1') {
                        $dataRow.$colName = $true
                    } else {
                        $currentRecordErrors += "Failed to convert '$csvValue' to Boolean for column '$colName'."
                        $dataRow.$colName = [System.DBNull]::Value # Set to NULL on conversion failure
                    }
                } elseif ($targetType -eq [System.Double]) {
                    # Convert to float/double
                    $doubleValue = 0.0
                    if ([double]::TryParse($csvValue, [ref]$doubleValue)) {
                        $dataRow.$colName = $doubleValue
                    } else {
                        $currentRecordErrors += "Failed to convert '$csvValue' to Double for column '$colName'."
                        $dataRow.$colName = [System.DBNull]::Value
                    }
                } elseif ($targetType -eq [System.DateTime]) {
                    # Convert to datetime2
                    $dateTimeValue = [System.DateTime]::MinValue
                    $parsedSuccessfully = $false

                    # Try parsing with time first
                    if ([System.DateTime]::TryParseExact($csvValue, $dateFormat1, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$dateTimeValue)) {
                        $parsedSuccessfully = $true
                    }
                    # If first format fails, try parsing without time
                    if (-not $parsedSuccessfully -and [System.DateTime]::TryParseExact($csvValue, $dateFormat2, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$dateTimeValue)) {
                        $parsedSuccessfully = true
                    }

                    if ($parsedSuccessfully) {
                        $dataRow.$colName = $dateTimeValue
                    } else {
                        $currentRecordErrors += "Failed to convert '$csvValue' to DateTime for column '$colName'."
                        $dataRow.$colName = [System.DBNull]::Value
                    }
                } elseif ($targetType -eq [System.Int32]) {
                    # Convert to integer (for ActiveTrackerCount)
                    $intValue = 0
                    if ([int]::TryParse($csvValue, [ref]$intValue)) {
                        $dataRow.$colName = $intValue
                    } else {
                        $currentRecordErrors += "Failed to convert '$csvValue' to Int32 for column '$colName'."
                        $dataRow.$colName = [System.DBNull]::Value
                    }
                }
                else {
                    # Default for string types
                    $dataRow.$colName = $csvValue
                }
            }
            catch {
                $currentRecordErrors += "Error processing column '$colName' with value '$csvValue': $($_.Exception.Message)"
                $dataRow.$colName = [System.DBNull]::Value # Ensure null on unexpected error
            }
        }

        # Add row to DataTable if there are no critical errors
        if ($currentRecordErrors.Count -eq 0) {
            $dataTable.Rows.Add($dataRow)
        } else {
            Write-Warning "Skipping record (Id: $($row.Id)) due to conversion errors:"
            $currentRecordErrors | ForEach-Object { Write-Warning "- $_" }
        }
        $processedRecords++
        # Removed the intermediate "Processed X records..." message from inside the loop
        # as the batching mechanism will give more relevant progress updates.
    }
    Write-Host "Finished populating DataTable with $($dataTable.Rows.Count) rows."


    # --- Perform Bulk Copy with Batching ---
    Write-Host "Initiating SqlBulkCopy with batch size $BatchSize..."
    $sqlBulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($connectionString, [System.Data.SqlClient.SqlBulkCopyOptions]::KeepNulls)
    $sqlBulkCopy.DestinationTableName = $TableName
    $sqlBulkCopy.BatchSize = $BatchSize # Set the batch size

    # Map all source (DataTable) columns to destination (SQL Table) columns
    # Ensure source column names match destination column names
    foreach ($col in $dataTable.Columns) {
        $sqlBulkCopy.ColumnMappings.Add($col.ColumnName, $col.ColumnName) | Out-Null
    }

    # Use WriteToServer(DataTable) directly as it handles batching internally when BatchSize is set.
    $sqlBulkCopy.WriteToServer($dataTable)

    $sqlBulkCopy.Close()

    Write-Host "CSV data successfully imported into [$TableName] table."

}
catch [System.IO.FileNotFoundException] {
    Write-Error "Error: The CSV file was not found at '$CsvFilePath'. $($_.Exception.Message)"
}
catch [System.Data.SqlClient.SqlException] {
    Write-Error "SQL Server Error: $($_.Exception.Message)"
    Write-Error "Error Code: $($_.Exception.ErrorCode)"
    Write-Error "SQL State: $($_.Exception.SqlState)"
}
catch {
    Write-Error "An unexpected error occurred: $($_.Exception.Message)"
    Write-Error $_.ScriptStackTrace
}
