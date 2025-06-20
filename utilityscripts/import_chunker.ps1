# PowerShell Script to chunk a large CSV file into smaller CSV files.
# Each smaller CSV file will contain a configurable number of records (default 10,000),
# and will include the original CSV header.

<#
.SYNOPSIS
    Splits a large CSV file into multiple smaller CSV files.

.DESCRIPTION
    This script reads a specified CSV file, determines its header,
    and then divides the records into smaller CSV files. Each output
    CSV file will have at most the specified number of records (default 10,000)
    and will include the original header row. The chunked files are saved
    into a new subfolder named after the original CSV file.

.PARAMETER InputCsvFilePath
    The full path to the large CSV file that needs to be chunked.

.PARAMETER MaxRecordsPerFile
    The maximum number of data records (excluding header) allowed in each
    output CSV file. Defaults to 10000.

.EXAMPLE
    # Split 'LargeContacts.csv' into files with max 10000 records each
    .\import_chunker.ps1 -InputCsvFilePath "C:\Data\LargeContacts.csv"

.EXAMPLE
    # Split 'SalesData.csv' into files with max 5000 records each
    .\import_chunker.ps1 -InputCsvFilePath "C:\Reports\SalesData.csv" -MaxRecordsPerFile 5000

.NOTES
    - The script will create a new subfolder in the same directory as the input CSV.
    - Output CSV files will be named like 'originalFileName_chunk_001.csv', etc.
    - Ensure the input CSV has a header row.
    - Uses -NoTypeInformation with Export-Csv to keep the output clean.
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$InputCsvFilePath,

    [int]$MaxRecordsPerFile = 10000 # Default batch size for chunking
)

Write-Host "Starting CSV chunking process..."
Write-Host "Input CSV File: $InputCsvFilePath"
Write-Host "Max Records Per Output File: $MaxRecordsPerFile"

try {
    # --- Validate Input CSV File ---
    if (-not (Test-Path -Path $InputCsvFilePath -PathType Leaf)) {
        Write-Error "Error: Input CSV file not found at '$InputCsvFilePath'."
        exit 1
    }

    # --- Get CSV Header and All Records ---
    # Read the entire CSV content to get the header and then process records
    # This approach is simple but might consume more memory for extremely large files.
    # For truly massive files (10s or 100s of millions of rows), a more stream-based
    # approach using .NET's StreamReader might be necessary.
    Write-Host "Reading input CSV file to extract header and records..."
    $csvContent = Get-Content -Path $InputCsvFilePath -ReadCount 0 # Read all lines
    
    if ($csvContent.Length -eq 0) {
        Write-Warning "The input CSV file '$InputCsvFilePath' is empty. No output files will be created."
        exit 0
    }

    $header = $csvContent[0]
    $dataRows = $csvContent | Select-Object -Skip 1

    if ($dataRows.Length -eq 0) {
        Write-Warning "The input CSV file '$InputCsvFilePath' contains only a header. No data rows to chunk."
        exit 0
    }

    # --- Create Output Directory ---
    $inputFileNameWithoutExtension = [System.IO.Path]::GetFileNameWithoutExtension($InputCsvFilePath)
    $inputDirectory = [System.IO.Path]::GetDirectoryName($InputCsvFilePath)
    $outputSubFolder = Join-Path -Path $inputDirectory -ChildPath "$($inputFileNameWithoutExtension)_chunks"

    if (-not (Test-Path -Path $outputSubFolder -PathType Container)) {
        New-Item -Path $outputSubFolder -ItemType Directory -Force | Out-Null
        Write-Host "Created output directory: '$outputSubFolder'"
    } else {
        Write-Warning "Output directory '$outputSubFolder' already exists. Existing files might be overwritten."
    }

    # --- Chunking Logic ---
    $chunkNumber = 1
    $recordsInCurrentChunk = 0
    $currentChunkData = @()
    $totalRecordsProcessed = 0

    Write-Host "Starting chunking of $($dataRows.Length) data records..."

    foreach ($line in $dataRows) {
        # Convert each line back to a PSObject if it was not already imported as such
        # We need to re-import line by line to ensure PSObject creation for Export-Csv
        # This is a less memory-intensive way than Import-Csving the whole file,
        # but slower than Import-Csving the whole file and then slicing.
        # For this script, given the requirement for header injection on each small CSV,
        # treating `dataRows` as an array of strings and then `ConvertFrom-Csv`
        # each chunk is more appropriate.

        # However, to avoid parsing each line separately for every record
        # a more efficient way for this specific problem (splitting a pre-read array)
        # is to recreate the objects in batches.

        # Re-importing a small chunk at a time using Import-Csv is simplest:
        $currentChunkData += $line
        $recordsInCurrentChunk++
        $totalRecordsProcessed++

        if ($recordsInCurrentChunk -eq $MaxRecordsPerFile -or $totalRecordsProcessed -eq $dataRows.Length) {
            # Time to write a chunk
            $outputFileName = Join-Path -Path $outputSubFolder -ChildPath "$($inputFileNameWithoutExtension)_chunk_$($chunkNumber.ToString('000')).csv"

            # Re-creating a temporary CSV string including the header for Export-Csv
            # This is key to ensure the header is present in each chunk.
            $chunkCsvString = @($header) + $currentChunkData | Out-String

            # Convert the chunk string back to PSObjects for Export-Csv
            $chunkObjects = $chunkCsvString | ConvertFrom-Csv -Delimiter "," # Assuming comma delimited, adjust if needed

            Write-Host "Writing chunk $($chunkNumber.ToString('000')) with $($chunkObjects.Count) records to '$outputFileName'..."
            $chunkObjects | Export-Csv -Path $outputFileName -NoTypeInformation -Force -Encoding UTF8 | Out-Null

            # Reset for next chunk
            $currentChunkData = @()
            $recordsInCurrentChunk = 0
            $chunkNumber++
        }
    }

    Write-Host "CSV chunking completed. Created $($chunkNumber - 1) chunk files in '$outputSubFolder'."

}
catch {
    Write-Error "An unexpected error occurred during CSV chunking: $($_.Exception.Message)"
    Write-Error $_.ScriptStackTrace
}
