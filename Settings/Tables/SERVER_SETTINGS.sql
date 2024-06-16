CREATE TABLE [Settings].[SERVER_SETTINGS] (
    [ID]          INT              IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
    [Client_ID]   INT              NULL,
    [Server_Name] NVARCHAR (500)   NOT NULL,
    [Module_ID]   UNIQUEIDENTIFIER NULL,
    [Key]         NVARCHAR (255)   NOT NULL,
    [Value]       NVARCHAR (4000)  NULL,
    [Updated]     DATETIME         NOT NULL
);

