CREATE TABLE [dbo].[XEventSessions] (
    [ID]               INT            IDENTITY (1, 1) NOT FOR REPLICATION NOT NULL,
    [XEventName]       NVARCHAR (100) NULL,
    [Last_Access_Time] DATETIME       NULL
);

