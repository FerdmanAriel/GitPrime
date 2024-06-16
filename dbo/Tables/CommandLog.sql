CREATE TABLE [dbo].[CommandLog] (
    [ID]              INT             IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
    [DatabaseName]    [sysname]       NULL,
    [SchemaName]      [sysname]       NULL,
    [ObjectName]      [sysname]       NULL,
    [ObjectType]      CHAR (2)        NULL,
    [IndexName]       [sysname]       NULL,
    [IndexType]       TINYINT         NULL,
    [StatisticsName]  [sysname]       NULL,
    [PartitionNumber] INT             NULL,
    [ExtendedInfo]    XML             NULL,
    [Command]         NVARCHAR (4000) NULL,
    [CommandType]     NVARCHAR (60)   NULL,
    [StartTime]       DATETIME        NULL,
    [EndTime]         DATETIME        NULL,
    [ErrorNumber]     INT             NULL,
    [ErrorMessage]    NVARCHAR (4000) NULL
);

