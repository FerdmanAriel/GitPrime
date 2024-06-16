﻿--EZMANAGE_
CREATE PROCEDURE [dbo].[SP_ATTACH_DB] 
-- Old name: EP_ATTACH_DB
 @ATTACH_DB NVARCHAR(128),
 @FILE_NAME NVARCHAR(600)
--WITH ENCRYPTION
AS

IF EXISTS (SELECT [name] FROM master..sysdatabases WHERE [name] = @ATTACH_DB) RAISERROR(N'Database being attached already exist on the server', 16, 1)

DECLARE @CMD_MDF NVARCHAR(3200)
DECLARE @CMD_ATTACH NVARCHAR(3200)
DECLARE @ATTACH_CNT INT
DECLARE @LOGICAL_NAME NVARCHAR(128)
DECLARE @PHYSICAL_NAME NVARCHAR(600)

SET @ATTACH_CNT = 0
SET @CMD_MDF = N'DBCC CHECKPRIMARYFILE (N'''+@FILE_NAME+''', 3)'

CREATE TABLE #tblMDF (
 status int,
 fileid int,
 [name] nvarchar(128),
 [filename] nvarchar(600)
)

INSERT INTO #tblMDF EXEC master..sp_executesql @CMD_MDF

IF (SELECT COUNT(*) FROM #tblMDF WHERE LOWER(RIGHT(RTRIM(LTRIM([filename])), 3)) != N'ldf') = 0 RAISERROR(N'Failed to retrieve database information from MDF file', 16, 1)
IF (SELECT COUNT(*) FROM #tblMDF WHERE LOWER(RIGHT(RTRIM(LTRIM([filename])), 3)) != N'ldf') = 1
BEGIN
 SET @CMD_ATTACH = N'EXEC master..sp_attach_single_file_db @dbname = N'''+@ATTACH_DB+''', @physname = N'''+@FILE_NAME+''''
END
 ELSE
BEGIN
 SET @CMD_ATTACH = N'EXEC master..sp_attach_db @dbname = N'''+@ATTACH_DB+''''
 DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
 FOR
 SELECT [name], [filename] FROM #tblMDF WHERE LOWER(RIGHT(RTRIM(LTRIM([filename])), 3)) != N'ldf' ORDER BY [filename] ASC
 OPEN CUR
 FETCH NEXT FROM CUR INTO @LOGICAL_NAME, @PHYSICAL_NAME
 WHILE @@FETCH_STATUS = 0
 BEGIN
  SET @ATTACH_CNT = @ATTACH_CNT + 1
  SET @CMD_ATTACH = @CMD_ATTACH+N', @filename'+CAST(@ATTACH_CNT AS NVARCHAR(80))+N' = N'''+RTRIM(LTRIM(@PHYSICAL_NAME))+''''
 FETCH NEXT FROM CUR INTO @LOGICAL_NAME, @PHYSICAL_NAME
 END
 CLOSE CUR
 DEALLOCATE CUR
END

EXEC master..sp_executesql @CMD_ATTACH