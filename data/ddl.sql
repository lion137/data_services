-- ==============================================
-- Table: DIRaw
-- ==============================================
CREATE TABLE [DIDashboard].[dbo].[DIRaw] (
    [Remediation_Owner]        varchar(255) COLLATE Latin1_General_CI_AS NULL,
    [PSID]                     varchar(80)  COLLATE Latin1_General_CI_AS NULL,
    [Remediation_Owner_Email]  varchar(120) COLLATE Latin1_General_CI_AS NULL,
    [Full_Path]                nvarchar(1800) COLLATE Latin1_General_CI_AS NULL,
    [Directory_Structure]      nvarchar(700)  COLLATE Latin1_General_CI_AS NULL,
    [Document_Name]            nvarchar(255)  COLLATE Latin1_General_CI_AS NULL,
    [DFS]                      nvarchar(1800) COLLATE Latin1_General_CI_AS NULL,
    [Created_Date]             varchar(20)  COLLATE Latin1_General_CI_AS NULL,
    [Modified_Date]            varchar(20)  COLLATE Latin1_General_CI_AS NULL,
    [Accessed_Date]            varchar(20)  COLLATE Latin1_General_CI_AS NULL,
    [creator_Name]             varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Owner_Name]               varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Owner_Login]              varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [extension]                varchar(10)  COLLATE Latin1_General_CI_AS NULL,
    [Owner_Email]              varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Modifier_Name]            varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Modifier_Login]           varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Modifier_Email]           varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Accessor_Name]            varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Accessor_Login]           varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Accessor_Email]           varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [Tags]                     varchar(500) COLLATE Latin1_General_CI_AS NULL,
    [Owner_Action]             varchar(120) COLLATE Latin1_General_CI_AS NULL,
    [Owner_Label]              varchar(120) COLLATE Latin1_General_CI_AS NULL,
    [Action_Timestamp]         varchar(80)  COLLATE Latin1_General_CI_AS NULL,
    [Action_Status]            varchar(20)  COLLATE Latin1_General_CI_AS NULL,
    [Classify_Time]            varchar(50)  COLLATE Latin1_General_CI_AS NULL,
    [Path_ID]                  int NULL,
    [Size_in_KB]               float NULL,
    [isensitive]               varchar(50)  COLLATE Latin1_General_CI_AS NULL,
    [id]                       uniqueidentifier DEFAULT NEWID() NOT NULL,
    [Ownership]                varchar(50)  COLLATE Latin1_General_CI_AS NULL,
    [Inferred_Owner_Level]     varchar(100) COLLATE Latin1_General_CI_AS NULL,
    [Load_Date]                datetime DEFAULT GETDATE() NOT NULL,
    [Load_For]                 varchar(30)  COLLATE Latin1_General_CI_AS NULL,
    [path_rowid]               bigint NULL,
    CONSTRAINT [PR_DIRaw_32123E83F4D652E66] PRIMARY KEY ([id])
);
GO

CREATE NONCLUSTERED INDEX [DIRaw_Ownership_idx]
ON [DIDashboard].[dbo].[DIRaw] ([Ownership] ASC)
WITH (
    PAD_INDEX = OFF,
    FILLFACTOR = 100,
    SORT_IN_TEMPDB = OFF,
    IGNORE_DUP_KEY = OFF,
    STATISTICS_NORECOMPUTE = OFF,
    ONLINE = OFF,
    ALLOW_ROW_LOCKS = ON,
    ALLOW_PAGE_LOCKS = ON
)
ON [PRIMARY];
GO


-- ==============================================
-- Table: FileOwner
-- ==============================================
CREATE TABLE [DIDashboard].[dbo].[FileOwner] (
    [PSID]        varchar(20)  COLLATE Latin1_General_CI_AS NOT NULL,
    [OwnerName]   varchar(150) COLLATE Latin1_General_CI_AS NULL,
    [OwnerEmail]  varchar(200) COLLATE Latin1_General_CI_AS NULL,
    CONSTRAINT [PK__FileOwne__BC0009768D14A389] PRIMARY KEY ([PSID])
);
GO


-- ==============================================
-- Table: FileOwnership
-- ==============================================
CREATE TABLE [DIDashboard].[dbo].[FileOwnership] (
    [ID]           uniqueidentifier DEFAULT NEWID() NOT NULL,
    [PSID]         varchar(20) COLLATE Latin1_General_CI_AS NOT NULL,
    [FileID]       uniqueidentifier NOT NULL,
    [PreviousOwner] varchar(50) COLLATE Latin1_General_CI_AS NULL,
    CONSTRAINT [PK__FileOwne__3214EC276BB4E804] PRIMARY KEY ([ID])
);
GO

CREATE NONCLUSTERED INDEX [FileOwnership_PSID_idx]
ON [dbo].[FileOwnership] ([PSID] ASC)
WITH (
    PAD_INDEX = OFF,
    FILLFACTOR = 100,
    SORT_IN_TEMPDB = OFF,
    IGNORE_DUP_KEY = OFF,
    STATISTICS_NORECOMPUTE = OFF,
    ONLINE = OFF,
    ALLOW_ROW_LOCKS = ON,
    ALLOW_PAGE_LOCKS = ON
)
ON [PRIMARY];
GO

ALTER TABLE [DIDashboard].[dbo].[FileOwnership]
ADD CONSTRAINT [FK__FileOwners__PSID__4E88ABD4]
FOREIGN KEY ([PSID])
REFERENCES [DIDashboard].[dbo].[FileOwner] ([PSID]);
GO

