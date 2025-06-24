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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SyncState](
	[StateName] [nvarchar](50) NOT NULL,
	[LastRecordId] [nvarchar](18) NULL,
	[LastSystemModstamp] [datetimeoffset](7) NULL,
	[LastUpdatedDateTime] [datetimeoffset](7) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [dbo].[SyncState] ADD PRIMARY KEY CLUSTERED 
(
	[StateName] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [dbo].[SyncState] ADD  DEFAULT (sysdatetimeoffset()) FOR [LastUpdatedDateTime]
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


-- ContentDocumentLinkSync

-- SQL to update the SyncState for 'ContentDocumentLinkSync' to June 15th, 2025, 4:00:00 AM UTC.
-- This MERGE statement will UPDATE the row if 'ContentDocumentLinkSync' exists,
-- or INSERT it if it does not exist.

MERGE INTO [dbo].[SyncState] AS T
USING (SELECT 'ContentDocumentLinkSync' AS StateName, '2024-01-01T04:00:00.000Z' AS TargetModstamp) AS S
ON T.StateName = S.StateName
WHEN MATCHED THEN
    UPDATE SET
        T.LastSystemModstamp = S.TargetModstamp,
        T.LastUpdatedDateTime = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (StateName, LastRecordId, LastSystemModstamp, LastUpdatedDateTime)
    VALUES (S.StateName, NULL, S.TargetModstamp, SYSDATETIMEOFFSET());



SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Contact](
	[Id] [nvarchar](18) NOT NULL,
	[IsDeleted] [bit] NULL,
	[MasterRecordId] [nvarchar](18) NULL,
	[AccountId] [nvarchar](18) NULL,
	[LastName] [nvarchar](80) NULL,
	[FirstName] [nvarchar](40) NULL,
	[Salutation] [nvarchar](255) NULL,
	[MiddleName] [nvarchar](40) NULL,
	[Suffix] [nvarchar](40) NULL,
	[Name] [nvarchar](121) NULL,
	[OtherStreet] [nvarchar](255) NULL,
	[OtherCity] [nvarchar](40) NULL,
	[OtherState] [nvarchar](80) NULL,
	[OtherPostalCode] [nvarchar](20) NULL,
	[OtherCountry] [nvarchar](80) NULL,
	[OtherStateCode] [nvarchar](255) NULL,
	[OtherCountryCode] [nvarchar](255) NULL,
	[OtherLatitude] [float] NULL,
	[OtherLongitude] [float] NULL,
	[OtherGeocodeAccuracy] [nvarchar](255) NULL,
	[OtherAddress] [nvarchar](max) NULL,
	[MailingStreet] [nvarchar](255) NULL,
	[MailingCity] [nvarchar](40) NULL,
	[MailingState] [nvarchar](80) NULL,
	[MailingPostalCode] [nvarchar](20) NULL,
	[MailingCountry] [nvarchar](80) NULL,
	[MailingStateCode] [nvarchar](255) NULL,
	[MailingCountryCode] [nvarchar](255) NULL,
	[MailingLatitude] [float] NULL,
	[MailingLongitude] [float] NULL,
	[MailingGeocodeAccuracy] [nvarchar](255) NULL,
	[MailingAddress] [nvarchar](max) NULL,
	[Phone] [nvarchar](40) NULL,
	[Fax] [nvarchar](40) NULL,
	[MobilePhone] [nvarchar](40) NULL,
	[HomePhone] [nvarchar](40) NULL,
	[OtherPhone] [nvarchar](40) NULL,
	[AssistantPhone] [nvarchar](40) NULL,
	[ReportsToId] [nvarchar](18) NULL,
	[Email] [nvarchar](80) NULL,
	[Title] [nvarchar](128) NULL,
	[Department] [nvarchar](80) NULL,
	[AssistantName] [nvarchar](40) NULL,
	[LeadSource] [nvarchar](255) NULL,
	[Birthdate] [datetime2](7) NULL,
	[Description] [nvarchar](max) NULL,
	[OwnerId] [nvarchar](18) NULL,
	[HasOptedOutOfEmail] [bit] NULL,
	[HasOptedOutOfFax] [bit] NULL,
	[DoNotCall] [bit] NULL,
	[ActionCadenceId] [nvarchar](18) NULL,
	[ActionCadenceAssigneeId] [nvarchar](18) NULL,
	[ActionCadenceState] [nvarchar](255) NULL,
	[ScheduledResumeDateTime] [datetime2](7) NULL,
	[ActiveTrackerCount] [int] NULL,
	[CreatedDate] [datetime2](7) NULL,
	[CreatedById] [nvarchar](18) NULL,
	[LastModifiedDate] [datetime2](7) NULL,
	[LastModifiedById] [nvarchar](18) NULL,
	[SystemModstamp] [datetime2](7) NULL,
	[LastActivityDate] [datetime2](7) NULL,
	[LastCURequestDate] [datetime2](7) NULL,
	[LastCUUpdateDate] [datetime2](7) NULL,
	[LastViewedDate] [datetime2](7) NULL,
	[LastReferencedDate] [datetime2](7) NULL,
	[EmailBouncedReason] [nvarchar](255) NULL,
	[EmailBouncedDate] [datetime2](7) NULL,
	[IsEmailBounced] [bit] NULL,
	[PhotoUrl] [nvarchar](255) NULL,
	[Jigsaw] [nvarchar](20) NULL,
	[JigsawContactId] [nvarchar](20) NULL,
	[FirstCallDateTime] [datetime2](7) NULL,
	[FirstEmailDateTime] [datetime2](7) NULL,
	[Pronouns] [nvarchar](255) NULL,
	[GenderIdentity] [nvarchar](255) NULL,
	[ActivityMetricId] [nvarchar](18) NULL,
	[ContactSource] [nvarchar](255) NULL,
	[TitleType] [nvarchar](255) NULL,
	[DepartmentGroup] [nvarchar](255) NULL,
	[BuyerAttributes] [nvarchar](max) NULL,
	[pi__Needs_Score_Synced__c] [bit] NULL,
	[pi__Pardot_Last_Scored_At__c] [datetime2](7) NULL,
	[pi__campaign__c] [nvarchar](255) NULL,
	[pi__comments__c] [nvarchar](max) NULL,
	[pi__conversion_date__c] [datetime2](7) NULL,
	[pi__conversion_object_name__c] [nvarchar](255) NULL,
	[pi__conversion_object_type__c] [nvarchar](255) NULL,
	[pi__created_date__c] [datetime2](7) NULL,
	[pi__first_activity__c] [datetime2](7) NULL,
	[pi__first_search_term__c] [nvarchar](255) NULL,
	[pi__first_search_type__c] [nvarchar](255) NULL,
	[pi__first_touch_url__c] [nvarchar](max) NULL,
	[pi__grade__c] [nvarchar](10) NULL,
	[pi__last_activity__c] [datetime2](7) NULL,
	[pi__notes__c] [nvarchar](max) NULL,
	[pi__pardot_hard_bounced__c] [bit] NULL,
	[pi__score__c] [float] NULL,
	[pi__url__c] [nvarchar](255) NULL,
	[pi__utm_campaign__c] [nvarchar](255) NULL,
	[pi__utm_content__c] [nvarchar](255) NULL,
	[pi__utm_medium__c] [nvarchar](255) NULL,
	[pi__utm_source__c] [nvarchar](255) NULL,
	[pi__utm_term__c] [nvarchar](255) NULL,
	[Last_Funded__c] [datetime2](7) NULL,
	[Last_Reassign_Date__c] [datetime2](7) NULL,
	[Last_Refresh_Date__c] [datetime2](7) NULL,
	[Last_Submission__c] [datetime2](7) NULL,
	[Loan_Submissions_All_Time__c] [float] NULL,
	[NMLS_ID__c] [nvarchar](255) NULL,
	[Ownership_Lock_Date__c] [datetime2](7) NULL,
	[Submissions_Last_30__c] [float] NULL,
	[Type__c] [nvarchar](1300) NULL,
	[nvb_Email_Flags__c] [nvarchar](max) NULL,
	[nvb_Email_Result__c] [nvarchar](255) NULL,
	[nvb_Email_Status__c] [nvarchar](255) NULL,
	[nvb_Last_Email_Check__c] [datetime2](7) NULL,
	[Ownership_Status__c] [nvarchar](1300) NULL,
	[nvb_Email_Check__c] [nvarchar](1300) NULL,
	[Broker_Approved_Date__c] [datetime2](7) NULL,
	[Do_Not_Email__c] [bit] NULL,
	[Modex_12_Months_Average_Monthly__c] [float] NULL,
	[Modex_12_Months_Average__c] [float] NULL,
	[Modex_12_Months_Count__c] [float] NULL,
	[Modex_12_Months_Sum__c] [float] NULL,
	[Modex_Branch_NMLS_Id__c] [nvarchar](255) NULL,
	[Modex_Company_NMLS_Id__c] [nvarchar](255) NULL,
	[Modex_Company_Name__c] [nvarchar](255) NULL,
	[Modex_Company_Website__c] [nvarchar](255) NULL,
	[Modex_Current_Job__c] [nvarchar](255) NULL,
	[Modex_Employer_Name__c] [nvarchar](255) NULL,
	[Modex_Facebook__c] [nvarchar](255) NULL,
	[Modex_Financial_Services_History__c] [nvarchar](255) NULL,
	[Modex_ID__c] [nvarchar](255) NULL,
	[Modex_Jobs_within_10_years__c] [nvarchar](255) NULL,
	[Modex_Linkedin__c] [nvarchar](255) NULL,
	[Modex_Office_Phone__c] [nvarchar](40) NULL,
	[Modex_Other_Email__c] [nvarchar](80) NULL,
	[Modex_Other_Phone__c] [nvarchar](40) NULL,
	[Modex_Personal_Email__c] [nvarchar](80) NULL,
	[Modex_Score__c] [float] NULL,
	[Modex_Transaction_Summary__c] [nvarchar](255) NULL,
	[Modex_Twitter__c] [nvarchar](255) NULL,
	[Modex_Type__c] [nvarchar](255) NULL,
	[Modex_URL__c] [nvarchar](255) NULL,
	[Modex_Work_Email__c] [nvarchar](80) NULL,
	[Modex_Zillow__c] [nvarchar](255) NULL,
	[Funded_Last_365__c] [float] NULL,
	[pi_Weekly_Greeting__c] [nvarchar](max) NULL,
	[Capitalization_Issue__c] [bit] NULL,
	[Phone_Check__c] [nvarchar](1300) NULL,
	[Primary_Campaign__c] [nvarchar](255) NULL,
	[Connects__c] [float] NULL,
	[Last_Connect__c] [datetime2](7) NULL,
	[Non_QM_Avg_Loan_Last_12__c] [float] NULL,
	[Non_QM_Percent_of_Total__c] [float] NULL,
	[Non_QM_Units_Last_12__c] [float] NULL,
	[Non_QM_Volume_Last_12__c] [float] NULL,
	[Lexi_s_Thoughts__c] [nvarchar](max) NULL,
	[Lexi_Score_Number__c] [float] NULL,
	[Case_Safe_ID__c] [nvarchar](1300) NULL,
	[Submissions_Last_365__c] [float] NULL,
	[Volume_Rank_LO__c] [float] NULL,
	[Volume_Score_Loan_Originator__c] [float] NULL,
	[Volume_Last_365__c] [float] NULL,
	[Dialpad__IsCreatedFromDialpad__c] [bit] NULL,
	[Dialpad__Powerdialer_Assigned_List__c] [nvarchar](1000) NULL,
	[Dialpad__Powerdialer_Dialed_List__c] [nvarchar](1000) NULL,
	[Dialpad__Powerdialer_Last_Dialed_via__c] [nvarchar](18) NULL,
	[Dialpad__Timezone__c] [nvarchar](255) NULL,
	[Dialpad__TotalNumberOfTimesDialed__c] [float] NULL,
	[Distance__c] [float] NULL,
	[Zoho_LO_ID__c] [nvarchar](255) NULL,
	[Last_Score_Calculation_Date__c] [datetime2](7) NULL,
	[LexiScoreVersion__c] [nvarchar](5) NULL,
	[Top_Lender__c] [nvarchar](255) NULL,
	[Top_Lender_Volume__c] [float] NULL,
	[Pull_Through__c] [float] NULL,
	[epbLexiScore__c] [float] NULL,
	[Follow_Up_Date__c] [datetime2](7) NULL,
	[Powerdial__c] [bit] NULL,
	[Total_Funded__c] [float] NULL,
	[Total_Funded_Units__c] [float] NULL,
	[Total_Submissions__c] [float] NULL,
	[Knowledge_Rating__c] [float] NULL,
	[Preparedness_Rating__c] [float] NULL,
	[Responsiveness_Rating__c] [float] NULL,
	[MCAE_Company_Name__c] [nvarchar](1300) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [dbo].[Contact] ADD  CONSTRAINT [PK_dbo.Contact] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
