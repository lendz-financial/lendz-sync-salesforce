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