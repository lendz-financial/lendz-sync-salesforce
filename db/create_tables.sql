SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ContentVersion]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[ContentVersion](
    [Id] [nvarchar](18) NOT NULL,
    [ContentDocumentId] [nvarchar](18) NULL,
    [IsLatest] [bit] NULL,
    [ContentUrl] [nvarchar](1333) NULL,
    [ContentBodyId] [nvarchar](18) NULL,
    [VersionNumber] [nvarchar](20) NULL,
    [Title] [nvarchar](255) NULL,
    [Description] [nvarchar](1000) NULL,
    [ReasonForChange] [nvarchar](1000) NULL,
    [SharingOption] [nvarchar](255) NULL,
    [SharingPrivacy] [nvarchar](255) NULL,
    [PathOnClient] [nvarchar](500) NULL,
    [RatingCount] [int] NULL,
    [IsDeleted] [bit] NULL,
    [ContentModifiedDate] [datetime2](7) NULL,
    [ContentModifiedById] [nvarchar](18) NULL,
    [PositiveRatingCount] [int] NULL,
    [NegativeRatingCount] [int] NULL,
    [FeaturedContentBoost] [int] NULL,
    [FeaturedContentDate] [datetime2](7) NULL,
    [OwnerId] [nvarchar](18) NULL,
    [CreatedById] [nvarchar](18) NULL,
    [CreatedDate] [datetime2](7) NULL,
    [LastModifiedById] [nvarchar](18) NULL,
    [LastModifiedDate] [datetime2](7) NULL,
    [SystemModstamp] [datetime2](7) NULL,
    [TagCsv] [nvarchar](2000) NULL,
    [FileType] [nvarchar](20) NULL,
    [PublishStatus] [nvarchar](255) NULL,
    [ContentSize] [int] NULL,
    [FileExtension] [nvarchar](40) NULL,
    [FirstPublishLocationId] [nvarchar](18) NULL,
    [Origin] [nvarchar](255) NULL,
    [NetworkId] [nvarchar](18) NULL,
    [ContentLocation] [nvarchar](255) NULL,
    [TextPreview] [nvarchar](255) NULL,
    [ExternalDocumentInfo1] [nvarchar](1000) NULL,
    [ExternalDocumentInfo2] [nvarchar](1000) NULL,
    [Checksum] [nvarchar](50) NULL,
    [IsMajorVersion] [bit] NULL,
    [IsAssetEnabled] [bit] NULL,
    [VersionDataUrl] [nvarchar](255) NULL
) ON [PRIMARY]
END
GO

SET ANSI_PADDING ON
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[ContentVersion]') AND name = N'PK_dbo.ContentVersion')
BEGIN
ALTER TABLE [dbo].[ContentVersion] ADD  CONSTRAINT [PK_dbo.ContentVersion] PRIMARY KEY CLUSTERED 
(
    [Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
END
GO

-- Add the new AzureBlobUrl column if it doesn't exist
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ContentVersion]') AND type in (N'U'))
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[ContentVersion]') AND name = N'AzureBlobUrl')
    BEGIN
        ALTER TABLE [dbo].[ContentVersion] ADD [AzureBlobUrl] [nvarchar](1000) NULL;
    END
END
GO

-- Create the SyncState table if it does not exist, or alter it if it exists with old column names
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SyncState]') AND type in (N'U'))
BEGIN
    PRINT 'Creating table [dbo].[SyncState] with generic columns...'
    CREATE TABLE [dbo].[SyncState](
        [StateName] NVARCHAR(50) NOT NULL PRIMARY KEY, -- A unique name for each sync process (e.g., 'ContentVersionSync', 'AccountSync')
        [LastRecordId] NVARCHAR(18) NULL,             -- The Salesforce 18-char Id of the last processed record for this StateName (generic)
        [LastSystemModstamp] DATETIMEOFFSET(7) NULL,   -- The SystemModstamp of the last processed record, for robust chronological syncing (generic)
        [LastUpdatedDateTime] DATETIMEOFFSET(7) DEFAULT SYSDATETIMEOFFSET() -- When this specific state record was last updated
    ) ON [PRIMARY]
    PRINT 'Table [dbo].[SyncState] created successfully.'
END
ELSE
BEGIN
    PRINT 'Table [dbo].[SyncState] already exists. Checking for schema updates...'

    -- Check if the old column 'LastContentVersionId' exists and drop it
    IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[SyncState]') AND name = N'LastContentVersionId')
    BEGIN
        PRINT 'Dropping old column [LastContentVersionId] from [dbo].[SyncState]...'
        ALTER TABLE [dbo].[SyncState] DROP COLUMN [LastContentVersionId];
        PRINT 'Old column [LastContentVersionId] dropped.'
    END

    -- Check if the new column 'LastRecordId' exists and add it if not
    IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[SyncState]') AND name = N'LastRecordId')
    BEGIN
        PRINT 'Adding new column [LastRecordId] to [dbo].[SyncState]...'
        ALTER TABLE [dbo].[SyncState] ADD [LastRecordId] NVARCHAR(18) NULL;
        PRINT 'New column [LastRecordId] added.'
    END

    -- Ensure LastSystemModstamp is DATETIMEOFFSET(7) (if type conversion is needed)
    -- This part is illustrative, full type alteration with data conversion is more complex.
    -- For simplicity, assume LastSystemModstamp is already correct from previous versions.
END
GO

-- Optional: Ensure a record for 'ContentVersionSync' exists in the SyncState table.
-- This uses MERGE for an UPSERT operation.
MERGE INTO [dbo].[SyncState] AS T
USING (SELECT 'ContentVersionSync' AS StateName, NULL AS LastId, NULL AS LastStamp) AS S
ON T.StateName = S.StateName
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StateName, LastRecordId, LastSystemModstamp)
    VALUES (S.StateName, S.LastId, S.LastStamp);
GO

-- SQL to update the SyncState for 'ContentVersionSync' to June 15th, 2025, 4:00:00 AM UTC.
-- This MERGE statement will UPDATE the row if 'ContentVersionSync' exists,
-- or INSERT it if it does not exist.

MERGE INTO [dbo].[SyncState] AS T
USING (SELECT 'ContentVersionSync' AS StateName, '2025-06-15T04:00:00.000Z' AS TargetModstamp) AS S
ON T.StateName = S.StateName
WHEN MATCHED THEN
    UPDATE SET
        T.LastSystemModstamp = S.TargetModstamp,
        T.LastUpdatedDateTime = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StateName, LastRecordId, LastSystemModstamp, LastUpdatedDateTime)
    VALUES (S.StateName, NULL, S.TargetModstamp, SYSDATETIMEOFFSET());