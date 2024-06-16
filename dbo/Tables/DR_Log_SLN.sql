CREATE TABLE [dbo].[DR_Log_SLN] (
    [Database_Name] NVARCHAR (512)  NOT NULL,
    [File_Name]     NVARCHAR (2048) NOT NULL,
    [Insert_Date]   DATETIME        NOT NULL,
    [LSN]           NUMERIC (18)    NOT NULL
);

