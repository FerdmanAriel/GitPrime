CREATE TABLE [dbo].[ET_VERSION] (
    [id]      INT           IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
    [type]    NVARCHAR (20) NULL,
    [name]    NVARCHAR (80) NULL,
    [version] INT           NULL
);

