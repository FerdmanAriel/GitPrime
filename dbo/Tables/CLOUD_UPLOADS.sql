CREATE TABLE [dbo].[CLOUD_UPLOADS] (
    [ID]                   INT             IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
    [Client_ID]            INT             NOT NULL,
    [Server_Name]          NVARCHAR (2000) NOT NULL,
    [Database_Name]        NVARCHAR (2000) NOT NULL,
    [Backup_Type]          NVARCHAR (2048) NULL,
    [Local_File_Name]      NVARCHAR (2000) NULL,
    [Local_Full_File_Name] NVARCHAR (2024) NULL,
    [Remote_Folder]        NVARCHAR (2024) NULL,
    [Time_To_Keep]         NVARCHAR (2048) NULL,
    [Status]               NVARCHAR (200)  NULL,
    [Start_Upload]         DATETIME        NULL,
    [End_Upload]           DATETIME        NULL,
    [Is_Success]           BIT             NULL,
    [Exception]            NVARCHAR (2048) NULL,
    [Progress]             INT             NULL,
    [Operation_Type]       NVARCHAR (2000) NOT NULL,
    [Is_Compressed]        BIT             NOT NULL,
    [Compression_Level]    NVARCHAR (50)   NULL,
    [Is_Encrypted]         BIT             NOT NULL,
    [Encryption_Algorithm] NVARCHAR (50)   NULL,
    [Encryption_Password]  NVARCHAR (50)   NULL
);

