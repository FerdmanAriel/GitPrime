--EZMANAGE_
create PROCEDURE [dbo].[SP_DRP_SOURCE_SERVER]
-- Old name: SP_DRP_SOURCE_SERVER
 @SOURCE_DATABASE NVARCHAR(128),
 @DESTINATION_SERVER NVARCHAR(128),
 @DRP_TYPE NVARCHAR(20),
 @DESTINATION_DATABASE_NAME_SUFFIX NVARCHAR(128),
 @INITIALIZE_FULL_BACKUP_IF_NEEDED BIT,
 @DRP_FORCE_BACKUP BIT,
 @DRP_FORCE_BACKUP_ALWAYS BIT,
 @FORCE_BACKUP_LOCATION NVARCHAR(1200),
 @FORCE_BACKUP_COMPRESSION INT,
 @FORCE_BACKUP_TTL INT,
 @DESTINATION_SERVER_INTEGRATED BIT,
 @DESTINATION_SERVER_USERNAME NVARCHAR(1200),
 @DESTINATION_SERVER_PASSWORD NVARCHAR(1200),
 @USR_BLOCKSIZE INT = NULL,
 @USR_BUFFERCOUNT INT = NULL,
 @USR_MAXTRANSFERSIZE INT = NULL,
 @SHOW_MESSAGE_FLAG BIT = 0
--WITH ENCRYPTION
AS

SET NOCOUNT ON 

IF @USR_BLOCKSIZE = 0 SET @USR_BLOCKSIZE = NULL
IF @USR_BLOCKSIZE IS NOT NULL IF @USR_BLOCKSIZE > 65536 SET @USR_BLOCKSIZE = NULL
IF @USR_BLOCKSIZE IS NOT NULL IF @USR_BLOCKSIZE < 2048 SET @USR_BLOCKSIZE = NULL

IF @USR_BUFFERCOUNT = 0 SET @USR_BUFFERCOUNT = NULL

IF @USR_MAXTRANSFERSIZE = 0 SET @USR_MAXTRANSFERSIZE = NULL
IF @USR_MAXTRANSFERSIZE IS NOT NULL IF @USR_MAXTRANSFERSIZE > 4194304 SET @USR_MAXTRANSFERSIZE = NULL
IF @USR_MAXTRANSFERSIZE IS NOT NULL IF @USR_MAXTRANSFERSIZE < 65536 SET @USR_MAXTRANSFERSIZE = NULL

DECLARE @DESTINATION_SERVER_USERNAME_DEC VARCHAR(1024)
DECLARE @DESTINATION_SERVER_PASSWORD_DEC VARCHAR(1024)

DECLARE @DESTINATION_DB NVARCHAR(128)
DECLARE @FORCE_BACKUP_COMPRESSION_FLAG INT

DECLARE @CMD NVARCHAR(3200)
DECLARE @LAST_DATA_BACKUP_FILE NVARCHAR(1024)
DECLARE @LAST_DATA_BACKUP_COMPRESS_FLAG INT
DECLARE @DESTINATION_DB_LAST_RESTORE_DATE DATETIME
DECLARE @RESTORE_WITH_STANDBY_FLAG BIT

DECLARE @BK_TYPE NVARCHAR(20)
DECLARE @BK_POSITION INT
DECLARE @BK_PHYSICAL_DEVICE_NAME NVARCHAR(1024)
DECLARE @BK_FINISH_DATE DATETIME
DECLARE @BK_COMPRESSION_FLAG INT
DECLARE @DB_CMPT_LEVEL INT
DECLARE @RC INT
DECLARE @FILE_ACCESS_CHECK INT
DECLARE @ERR_MSG NVARCHAR(1024)
DECLARE @DESTINATION_DATABASE_COMPATIBILITY_LEVEL INT

DECLARE @SOURCE_TABLE_COUNT INT
DECLARE @SOURCE_ROW_COUNT BIGINT
DECLARE @SOURCE_SP_COUNT INT

DECLARE @DESTINATION_TABLE_COUNT INT
DECLARE @DESTINATION_ROW_COUNT BIGINT
DECLARE @DESTINATION_SP_COUNT INT
------------------------------------------

IF @DESTINATION_DATABASE_NAME_SUFFIX IS NULL SET @DESTINATION_DATABASE_NAME_SUFFIX = N''
IF @DRP_FORCE_BACKUP IS NULL SET @DRP_FORCE_BACKUP = 0
IF @DRP_FORCE_BACKUP_ALWAYS IS NULL SET @DRP_FORCE_BACKUP_ALWAYS = 0
SET @DESTINATION_DB = @SOURCE_DATABASE+@DESTINATION_DATABASE_NAME_SUFFIX

IF @FORCE_BACKUP_COMPRESSION IS NULL SET @FORCE_BACKUP_COMPRESSION = 1
IF @FORCE_BACKUP_COMPRESSION > 0 SET @FORCE_BACKUP_COMPRESSION_FLAG = 1 ELSE SET @FORCE_BACKUP_COMPRESSION_FLAG = 0

--First checking if a linked server exists to the destination server.
IF NOT EXISTS (SELECT * FROM master..sysservers WHERE [srvname] = @DESTINATION_SERVER)
BEGIN
 --The destination server doesnt exist as a linked server and needs to be added
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Adding linked server "'+ISNULL(@DESTINATION_SERVER, N'')+N'"...')
 EXEC master..sp_addlinkedserver @DESTINATION_SERVER, N'SQL Server'
END

--If this is NOT an integrated security to the linked server, we need to re-create the linked
--server using the user/pass supplied...
IF @DESTINATION_SERVER_INTEGRATED = 0
BEGIN
 --SQL authentication is used, encrypted user+pass will be used
 --The "cached" user+pass is removed and re-created...
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Re-registering linked server SQL authentication login...')
 EXEC master..sp_droplinkedsrvlogin @rmtsrvname = @DESTINATION_SERVER, @locallogin = NULL 
 IF (@DESTINATION_SERVER_USERNAME IS NOT NULL) AND (@DESTINATION_SERVER_USERNAME <> N'') EXEC master..xp_sql_key_decrypt @DESTINATION_SERVER_USERNAME, @DESTINATION_SERVER_USERNAME_DEC OUT ELSE SET @DESTINATION_SERVER_USERNAME_DEC = N''
 IF (@DESTINATION_SERVER_PASSWORD IS NOT NULL) AND (@DESTINATION_SERVER_PASSWORD <> N'') EXEC master..xp_sql_key_decrypt @DESTINATION_SERVER_PASSWORD, @DESTINATION_SERVER_PASSWORD_DEC OUT ELSE SET @DESTINATION_SERVER_PASSWORD_DEC = N''
 EXEC master..sp_addlinkedsrvlogin @rmtsrvname = @DESTINATION_SERVER, @useself = N'False', @locallogin = NULL, @rmtuser = @DESTINATION_SERVER_USERNAME_DEC, @rmtpassword = @DESTINATION_SERVER_PASSWORD_DEC
END

--Check the connection to the remote server
IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - SOURCE: Verifying remote server connection...')
SET @CMD = N'EXEC ['+@DESTINATION_SERVER+N'].[master].[dbo].[sp_executesql] N''PRINT N'''' - DESTINATION: Verifying remote server connection...'''''''
EXEC master..sp_executesql @CMD

--Getting the compatibility level of the source database
SELECT TOP 1 @DB_CMPT_LEVEL = [cmptlevel] FROM master..sysdatabases WHERE [name] = @SOURCE_DATABASE

--Getting the compatibility level of the destination server
PRINT(N' - Reading destination compatibility level...')
SET @CMD = N'SELECT @DESTINATION_DATABASE_COMPATIBILITY_LEVEL_OUT = [cmptlevel] FROM ['+ISNULL(@DESTINATION_SERVER, N'')+N'].[master].[dbo].sysdatabases WHERE [name] = N''model'''
EXEC master..sp_executesql @CMD, N'@DESTINATION_DATABASE_COMPATIBILITY_LEVEL_OUT INT OUTPUT', @DESTINATION_DATABASE_COMPATIBILITY_LEVEL OUTPUT
IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Destination database compatibility level: "'+CAST(ISNULL(@DESTINATION_DATABASE_COMPATIBILITY_LEVEL, 0) AS NVARCHAR(20))+N'"')

--Getting the last data backup file and the compression flag (according to the suffix)
SELECT TOP 1 
 @LAST_DATA_BACKUP_FILE = [physical_device_name],
 @LAST_DATA_BACKUP_COMPRESS_FLAG = (CASE CHARINDEX(N'.', REVERSE([physical_device_name])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([physical_device_name]), 1, CHARINDEX(N'.', REVERSE([physical_device_name])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) 
FROM msdb..backupmediafamily WHERE media_set_id = (SELECT TOP 1 [media_set_id] FROM msdb..backupset WHERE [database_name] = @SOURCE_DATABASE AND [type] = N'D' ORDER BY backup_finish_date DESC) ORDER BY [media_set_id] DESC

IF @SHOW_MESSAGE_FLAG = 1
BEGIN
 PRINT (N' (i) Reading source last DATA backup file')
 PRINT (N'   File: "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"')
 PRINT (N'   Compressed: "'+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESS_FLAG, 0) AS NVARCHAR(20))+N'"')
 PRINT (N'')
END

--first we need to check if the destination database exists, if not - create it from the last data backup
DECLARE @DESTINATION_COUNT INT
SET @DESTINATION_COUNT = 0
IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Checking if destination database exists...')
SET @CMD = N'SELECT @DESTINATION_COUNT_OUTPUT = COUNT(*) FROM ['+ISNULL(@DESTINATION_SERVER, N'')+N'].[master].[dbo].sysdatabases WHERE [name] = N'''+ISNULL(@DESTINATION_DB, N'')+N''''
EXEC master..sp_executesql @CMD, N'@DESTINATION_COUNT_OUTPUT INT OUTPUT', @DESTINATION_COUNT OUTPUT
IF @DESTINATION_COUNT <= 0
BEGIN
 --This means that the destination database DOESNT exist, and needs to be created.
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Destination database does NOT exist')
 IF (@LAST_DATA_BACKUP_FILE IS NULL) OR (@LAST_DATA_BACKUP_FILE = N'')
 BEGIN
  --This means that there is no DATA backup file on the source database
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) NO source database DATA backup exists')
  IF (@INITIALIZE_FULL_BACKUP_IF_NEEDED = 1) OR (@DRP_FORCE_BACKUP_ALWAYS = 1)
  BEGIN
   --No full backup exists, and needs to be initialized
   PRINT(N' - Initiating full database backup for the source database...')
   SET @CMD = N'EXEC [EZManagePro]..[SP_BACKUP] @DATABASE_NAME = N'''+ISNULL(@SOURCE_DATABASE, N'')+N''', @BACKUP_TYPE = N''D'', @LOCATION = N'''+ISNULL(@FORCE_BACKUP_LOCATION, N'')+N''', @COMPRESS = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION_FLAG, 1) AS NVARCHAR(4))+N', @TTL = '+CAST(ISNULL(@FORCE_BACKUP_TTL, 0) AS NVARCHAR(4))+N', @COMPRESSION_LEVEL = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION, 1) AS NVARCHAR(4))
   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
   IF @SHOW_MESSAGE_FLAG = 1 PRINT @CMD
   EXEC master..sp_executesql @CMD

   --When backup is completed - requering the last backup file of the source db...
   SELECT TOP 1 
    @LAST_DATA_BACKUP_FILE = [physical_device_name],
    @LAST_DATA_BACKUP_COMPRESS_FLAG = (CASE CHARINDEX(N'.', REVERSE([physical_device_name])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([physical_device_name]), 1, CHARINDEX(N'.', REVERSE([physical_device_name])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) 
   FROM msdb..backupmediafamily WHERE media_set_id = (SELECT TOP 1 [media_set_id] FROM msdb..backupset WHERE [database_name] = @SOURCE_DATABASE AND [type] = N'D' ORDER BY backup_finish_date DESC) ORDER BY [media_set_id] DESC

   IF @SHOW_MESSAGE_FLAG = 1
   BEGIN
    PRINT (N' (i) Reading source last DATA backup file')
    PRINT (N'   File: "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"')
    PRINT (N'   Compressed: "'+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESS_FLAG, 0) AS NVARCHAR(20))+N'"')
    PRINT (N'')
   END
  END
   ELSE
  BEGIN
   --The database cannot be created at this time (no DATA backup exists, the execution is aborted)
   RAISERROR(N'Source database has no DATA backup, DRP is aborted', 1, 1)
   RETURN
  END
 END

 --This is a local backup, we need to add the network server name instead of the local drive
 IF LEFT(@LAST_DATA_BACKUP_FILE, 2) != N'\\'
 BEGIN
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - This is a local backup, adding UNC prefix...')
  SET @LAST_DATA_BACKUP_FILE = N'\\'+CAST(SERVERPROPERTY(N'MachineName') AS NVARCHAR(128))+N'\'+LEFT(@LAST_DATA_BACKUP_FILE, 1)+N'$\'+RIGHT(@LAST_DATA_BACKUP_FILE, LEN(@LAST_DATA_BACKUP_FILE) - 3)
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N'   File: "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"')
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N'')
 END

 --Before the actual resotre, we try to verify the access to the backup file, so that if there is no access
 --we can generate a more "friendly" error message
 PRINT(N' - Verifying access to backup file from remote server...')
 SET @CMD = N'EXEC ['+@DESTINATION_SERVER+N'].[master].[dbo].[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
 EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT=@FILE_ACCESS_CHECK OUTPUT
 IF @FILE_ACCESS_CHECK = 0
 BEGIN
  --This means that there is NO access to the backup file...
  SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'" from server "'+ISNULL(@DESTINATION_SERVER, N'')+N'"'

  --no access to the full backup file, now we need to check if a full backup file should be initiated (if needed)
  IF (@INITIALIZE_FULL_BACKUP_IF_NEEDED = 1) OR (@DRP_TYPE = N'D' AND (@DRP_FORCE_BACKUP_ALWAYS = 1 OR @DRP_FORCE_BACKUP = 1))
  BEGIN
   --There is no access to the last data backup file, but a new one should be initiated...
   PRINT(@ERR_MSG)

   PRINT(N' - Initiating full database backup for the source database...')
   SET @CMD = N'EXEC [EZManagePro]..[SP_BACKUP] @DATABASE_NAME = N'''+ISNULL(@SOURCE_DATABASE, N'')+N''', @BACKUP_TYPE = N''D'', @LOCATION = N'''+ISNULL(@FORCE_BACKUP_LOCATION, N'')+N''', @COMPRESS = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION_FLAG, 1) AS NVARCHAR(4))+N', @TTL = '+CAST(ISNULL(@FORCE_BACKUP_TTL, 0) AS NVARCHAR(4))+N', @COMPRESSION_LEVEL = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION, 1) AS NVARCHAR(4))
   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
   IF @SHOW_MESSAGE_FLAG = 1 PRINT @CMD
   EXEC master..sp_executesql @CMD

   --When backup is completed - requering the last backup file of the source db...
   SELECT TOP 1 
    @LAST_DATA_BACKUP_FILE = [physical_device_name],
    @LAST_DATA_BACKUP_COMPRESS_FLAG = (CASE CHARINDEX(N'.', REVERSE([physical_device_name])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([physical_device_name]), 1, CHARINDEX(N'.', REVERSE([physical_device_name])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) 
   FROM msdb..backupmediafamily WHERE media_set_id = (SELECT TOP 1 [media_set_id] FROM msdb..backupset WHERE [database_name] = @SOURCE_DATABASE AND [type] = N'D' ORDER BY backup_finish_date DESC) ORDER BY [media_set_id] DESC
  END
   ELSE
  BEGIN
   --This means no backup file should be initiated, and the existing one cannot be accessed - so, error and
   --abort...
   RAISERROR(@ERR_MSG, 16, 1)
   RETURN
  END
 END

 --If we reached here this means that there IS access to the backup file...
 -------------------------------------------------------
 --Creating the database from the backup file...
 --Regarding the backup file we need to check - if this is a "local" backup file - we need to add the
 --network name...
 IF @DB_CMPT_LEVEL < @DESTINATION_DATABASE_COMPATIBILITY_LEVEL
  SET @RESTORE_WITH_STANDBY_FLAG = 0
 ELSE
  SET @RESTORE_WITH_STANDBY_FLAG = 1
 IF @DRP_TYPE = N'D' SET @RESTORE_WITH_STANDBY_FLAG = 1

 SET @CMD = N'EXEC ['+ISNULL(@DESTINATION_SERVER, N'')+N'].[EZManagePro].[dbo].[SP_RESTORE] @DATABASE_NAME = N'''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESS_FLAG, 1) AS NVARCHAR(4))+N', @NEW_DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB, N'')+N''', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@DB_CMPT_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
 IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
 IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
 IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
 IF @SHOW_MESSAGE_FLAG = 1 PRINT @CMD
 EXEC master..sp_executesql @CMD
 
 --If the replication type is DATA, the database have just been created from the last data backup - then we
 --finish this replication cycle.
 IF (@DRP_TYPE = N'D') RETURN
END
 ELSE
BEGIN
 --The destination database already exists...
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Destination database EXISTS')

 --we need to get the compatibility level for the standby flag...
  IF @DB_CMPT_LEVEL < @DESTINATION_DATABASE_COMPATIBILITY_LEVEL
   SET @RESTORE_WITH_STANDBY_FLAG = 0
  ELSE
   SET @RESTORE_WITH_STANDBY_FLAG = 1
  IF @DRP_TYPE = N'D' SET @RESTORE_WITH_STANDBY_FLAG = 1
END

--now we need to check if any new backups were made since our last restore...
IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Check if new source backup was made since last restore...')
SET @CMD = N'SELECT TOP 1 @DESTINATION_DB_LAST_RESTORE_DATE_OUT = [restore_date] FROM ['+ISNULL(@DESTINATION_SERVER, N'')+N'].[msdb].[dbo].restorehistory WHERE [destination_database_name] = N'''+ISNULL(@DESTINATION_DB, N'')+N''' ORDER BY restore_date DESC'
EXEC master..sp_executesql @CMD, N'@DESTINATION_DB_LAST_RESTORE_DATE_OUT DATETIME OUTPUT', @DESTINATION_DB_LAST_RESTORE_DATE OUTPUT
IF @SHOW_MESSAGE_FLAG = 1 PRINT (N'     Destination database last restore date: "'+CAST(ISNULL(@DESTINATION_DB_LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+N'"')

CREATE TABLE #tblRestore (
 [backup_finish_date] DATETIME,
 [type] NVARCHAR(20),
 [position] INT,
 [physical_device_name] NVARCHAR(1024),
 [compressed_flag] INT
)

--This is a local backup, we need to add the network server name instead of the local drive
IF LEFT(@LAST_DATA_BACKUP_FILE, 2) != N'\\'
BEGIN
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Adding UNC prefix to last data backup file...')
 SET @LAST_DATA_BACKUP_FILE = N'\\'+CAST(SERVERPROPERTY(N'MachineName') AS NVARCHAR(128))+N'\'+LEFT(@LAST_DATA_BACKUP_FILE, 1)+N'$\'+RIGHT(@LAST_DATA_BACKUP_FILE, LEN(@LAST_DATA_BACKUP_FILE) - 3)
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N'     File: "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"')
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N'')
END

IF @DRP_FORCE_BACKUP_ALWAYS = 1
BEGIN
 --This means that a backup needs to be initiated ALWAYS
 PRINT(N' - Backup is required (Force always), initializing backup execution...')
 SET @CMD = N'EXEC [EZManagePro]..[SP_BACKUP] @DATABASE_NAME = N'''+ISNULL(@SOURCE_DATABASE, N'')+N''', @BACKUP_TYPE = N'''+ISNULL(@DRP_TYPE, N'')+N''', @LOCATION = N'''+ISNULL(@FORCE_BACKUP_LOCATION, N'')+N''', @COMPRESS = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION_FLAG, 1) AS NVARCHAR(4))+N', @TTL = '+CAST(ISNULL(@FORCE_BACKUP_TTL, 0) AS NVARCHAR(4))+N', @COMPRESSION_LEVEL = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION, 1) AS NVARCHAR(4))
 IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
 IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
 IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
 IF @SHOW_MESSAGE_FLAG = 1 PRINT @CMD
 --The actual "force" backup command execution
 EXEC master..sp_executesql @CMD
END
 ELSE
BEGIN
 --This means that a backup needs to be initiated only if necessary
 IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) "Force always" is NOT set')
 IF @DRP_FORCE_BACKUP = 1
 BEGIN
  --If force replication is required, this means that if there is no relevant backup (new backup that needs to
  --be replicated to the destination database), a backup (of the replication type) will be initiated (and thus - forcing
  --a replication)
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) "Force if necessary" IS set, checking if backup is needed...')

  -----------------------------

  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Reading objects from SOURCE database...')
  SET @CMD = N'SET NOCOUNT ON SELECT @SOURCE_TABLE_COUNT = COUNT(obj.[name]), @SOURCE_ROW_COUNT = SUM(ind.[rowcnt]) 
   FROM ['+@SOURCE_DATABASE+N']..[sysobjects] obj INNER JOIN ['+ISNULL(@SOURCE_DATABASE, N'')+N']..[sysindexes] ind 
   ON obj.[id] = ind.[id] WHERE obj.[xtype] = N''U'' AND ind.[indid] IN (0, 1)'
  EXEC master..sp_executesql @CMD, N'@SOURCE_TABLE_COUNT INT OUTPUT, @SOURCE_ROW_COUNT BIGINT OUTPUT', @SOURCE_TABLE_COUNT OUTPUT, @SOURCE_ROW_COUNT OUTPUT

  SET @CMD = N'SELECT @SOURCE_SP_COUNT = COUNT(*) FROM ['+ISNULL(@SOURCE_DATABASE, N'')+N']..[sysobjects] WHERE [xtype] = N''P'''
  EXEC master..sp_executesql @CMD, N'@SOURCE_SP_COUNT INT OUTPUT', @SOURCE_SP_COUNT OUTPUT

  -----------------------------

  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Reading objects from DESTINATION database...')
  SET @CMD = N'SET NOCOUNT ON SELECT @DESTINATION_TABLE_COUNT = COUNT(obj.[name]), @DESTINATION_ROW_COUNT = SUM(ind.[rowcnt]) 
   FROM ['+ISNULL(@DESTINATION_SERVER, N'')+N'].['+ISNULL(@DESTINATION_DB, N'')+N'].[dbo].[sysobjects] obj INNER JOIN ['+ISNULL(@DESTINATION_SERVER, N'')+N'].['+ISNULL(@DESTINATION_DB, N'')+N'].[dbo].[sysindexes] ind 
   ON obj.[id] = ind.[id] WHERE obj.[xtype] = N''U'' AND ind.[indid] IN (0, 1)'
  EXEC master..sp_executesql @CMD, N'@DESTINATION_TABLE_COUNT INT OUTPUT, @DESTINATION_ROW_COUNT BIGINT OUTPUT', @DESTINATION_TABLE_COUNT OUTPUT, @DESTINATION_ROW_COUNT OUTPUT

  SET @CMD = N'SELECT @DESTINATION_SP_COUNT = COUNT(*) FROM ['+ISNULL(@DESTINATION_SERVER, N'')+N'].['+ISNULL(@DESTINATION_DB, N'')+N'].[dbo].[sysobjects] WHERE [xtype] = N''P'''
  EXEC master..sp_executesql @CMD, N'@DESTINATION_SP_COUNT INT OUTPUT', @DESTINATION_SP_COUNT OUTPUT
  
  -----------------------------

  IF (@SOURCE_ROW_COUNT <> @DESTINATION_ROW_COUNT) OR (@SOURCE_TABLE_COUNT <> @DESTINATION_TABLE_COUNT) OR (@SOURCE_SP_COUNT <> @DESTINATION_SP_COUNT)
  BEGIN
   --This means that a force backup will be initiated in order to force a replication...
   --the backup type will be as the type of the replication...
   PRINT(N' - Backup is required (Force if needed), initializing backup execution...')
   SET @CMD = N'EXEC [EZManagePro]..[SP_BACKUP] @DATABASE_NAME = N'''+ISNULL(@SOURCE_DATABASE, N'')+N''', @BACKUP_TYPE = N'''+ISNULL(@DRP_TYPE, N'')+N''', @LOCATION = N'''+ISNULL(@FORCE_BACKUP_LOCATION, N'')+N''', @COMPRESS = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION_FLAG, 1) AS NVARCHAR(4))+N', @TTL = '+CAST(ISNULL(@FORCE_BACKUP_TTL, 0) AS NVARCHAR(4))+N', @COMPRESSION_LEVEL = '+CAST(ISNULL(@FORCE_BACKUP_COMPRESSION, 1) AS NVARCHAR(4))
   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
   IF @SHOW_MESSAGE_FLAG = 1 PRINT @CMD
   --The actual "force" backup command execution
   EXEC master..sp_executesql @CMD
  END
   ELSE
  BEGIN
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' (i) Backup is NOT needed')
  END
 END
END

IF (@DESTINATION_DB_LAST_RESTORE_DATE < (SELECT TOP 1 [backup_finish_date] FROM msdb..backupset WHERE database_name = @SOURCE_DATABASE ORDER BY backup_finish_date DESC))
BEGIN
 --This means that the last backup on the source database occured after the last restore on the destination
 --database, which means we need to perform a restore on the destination database.

 --Now before we actually perform the restore, we'll check that we are restoring according
 --to the correct replication type.
 --If the replication type is 'D', we should ONLY use the 'D' type backups, but on the other
 --hand, if our replication is 'L' or 'I' - we use that specific type and also the 'D'

 --For 'D' replication - we'll need only the last data backup file
 --For 'L' replication - we'll need the last data backup and all the logs that follows it
 --For 'I' replication - we'll need the last data backup and the last differential that follows it

 IF (@DRP_TYPE = N'D') OR (@DRP_TYPE = N'I') OR (@DRP_TYPE = N'L')
 BEGIN

  --The cursor will run on all backups that are newer than the last restore data of
  --the destination database, but we will populate the final restore table with only
  --the relevant values
  DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
  FOR
   SELECT 
    bkset.[backup_finish_date],
    bkset.[type],
    bkset.[position], 
    bkfam.[physical_device_name],
   (CASE CHARINDEX(N'.', REVERSE([physical_device_name])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([physical_device_name]), 1, CHARINDEX(N'.', REVERSE([physical_device_name])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS [compression_flag]
   FROM 
    msdb..backupset bkset
   INNER JOIN
    msdb..backupmediafamily bkfam
  	ON bkset.media_set_id = bkfam.media_set_id
   WHERE 
    bkset.[database_name] = @SOURCE_DATABASE 
    AND bkset.[backup_finish_date] > @DESTINATION_DB_LAST_RESTORE_DATE
   ORDER BY 
    bkset.backup_finish_date ASC
  OPEN CUR
  FETCH NEXT FROM CUR INTO @BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME, @BK_COMPRESSION_FLAG
  WHILE @@FETCH_STATUS = 0
  BEGIN
   --------------------------------------------
   --Monitor: see the acutal items generating the final restore table...
   --PRINT(@BK_TYPE+N', '+@BK_PHYSICAL_DEVICE_NAME)

   IF @DRP_TYPE = N'D'
   BEGIN
    ---------------------------
    --This is a DATA replication, we need to get the last DATA backup that is newer than the last
    --restore of the destination database.

    --Because we need only the last DATA backup - if this is a DATA backup - the previous one is irrelevant
    IF @BK_TYPE = N'D' 
    BEGIN
     DELETE #tblRestore
     INSERT INTO #tblRestore ([backup_finish_date], [type], [position], [physical_device_name], [compressed_flag])
      VALUES (@BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME, @BK_COMPRESSION_FLAG)
    END
    ---------------------------
   END

   IF @DRP_TYPE = N'L'
   BEGIN
    ---------------------------
    --This is a LOG replication, we need to get all LOG files newer than the last restore of the
    --destination database, BUT, if a DATA backup exists in the sequence - we only need the log files
    --that are after that DATA

    --If we encounter a DATA backup, than all previous backups are irrelevant
    IF @BK_TYPE = N'D' DELETE #tblRestore
    IF (@BK_TYPE = N'D') OR (@BK_TYPE = N'L')
    BEGIN
     INSERT INTO #tblRestore ([backup_finish_date], [type], [position], [physical_device_name], [compressed_flag])
      VALUES (@BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME, @BK_COMPRESSION_FLAG)
    END
    ---------------------------
   END

   IF @DRP_TYPE = N'I'
   BEGIN
    ---------------------------
    --This is a DIFF replication, we need to get all DIFF files newer than the last restore of the
    --destination database, BUT, if a DATA backup exists in the sequence - we only need the diff files
    --that are after that DATA

    --If we encounter a DATA backup, than all previous backups are irrelevant,
    --and if we encounter a differential backup - this means that all other differential backups are irrelevant.
    IF @BK_TYPE = N'D' DELETE #tblRestore
    IF @BK_TYPE = N'I' DELETE #tblRestore WHERE [type] = N'I'
    IF (@BK_TYPE = N'D') OR (@BK_TYPE = N'I')
    BEGIN
     INSERT INTO #tblRestore ([backup_finish_date], [type], [position], [physical_device_name], [compressed_flag])
      VALUES (@BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME, @BK_COMPRESSION_FLAG)
    END
    ---------------------------
   END
   --------------------------------------------
  FETCH NEXT FROM CUR INTO @BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME, @BK_COMPRESSION_FLAG
  END
  CLOSE CUR
  DEALLOCATE CUR

  --Monitor: seperator...
  --PRINT(N'------------------------------------------------------')

  --NOW - after we generated the actual relevant restore table, we'll run a cursor on it for
  --generating the actual relevant restore commands on the remote (destination) database.
  DECLARE CUR_RES CURSOR LOCAL FORWARD_ONLY READ_ONLY
  FOR
   SELECT [backup_finish_date], [type], [position], [physical_device_name] FROM #tblRestore ORDER BY [backup_finish_date] ASC
  OPEN CUR_RES
  FETCH NEXT FROM CUR_RES INTO @BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME
  WHILE @@FETCH_STATUS = 0
  BEGIN
   --If this is a local backup, we need to add the network server name instead of the local drive
   IF LEFT(@BK_PHYSICAL_DEVICE_NAME, 2) != N'\\'
    SET @BK_PHYSICAL_DEVICE_NAME = N'\\'+CAST(SERVERPROPERTY(N'MachineName') AS NVARCHAR(128))+N'\'+LEFT(@BK_PHYSICAL_DEVICE_NAME, 1)+N'$\'+RIGHT(@BK_PHYSICAL_DEVICE_NAME, LEN(@BK_PHYSICAL_DEVICE_NAME) - 3)

   --Before actually restoring the file, we want to first verify access to the backup file from the remote server
   PRINT(N' - Verifying access to backup file from remote server...')
   SET @CMD = N'EXEC ['+ISNULL(@DESTINATION_SERVER, N'')+N'].[master].[dbo].[xp_fileexist] N'''+ISNULL(@BK_PHYSICAL_DEVICE_NAME, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
   EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT=@FILE_ACCESS_CHECK OUTPUT
   IF @FILE_ACCESS_CHECK = 0
   BEGIN
    --This means that there is no access to the backup file from the remote server...
    SET @ERR_MSG = N'Unable to access backup file "'+@BK_PHYSICAL_DEVICE_NAME+N'" from server "'+@DESTINATION_SERVER+N'"'
    RAISERROR(@ERR_MSG, 16, 1)
   END
    ELSE
   BEGIN
    --This means that there IS access to the backup file from the remote server, we can continue with the operation...

    --Information, the file being restored
    PRINT(N' - DESTINATION: Restoring file ('+ISNULL(@BK_TYPE, N'')+N'): "'+ISNULL(@BK_PHYSICAL_DEVICE_NAME, N'')+N'"')
    SET @CMD = N'EXEC ['+ISNULL(@DESTINATION_SERVER, N'')+N'].[EZManagePro].[dbo].[SP_RESTORE] @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB, N'')+N''', @BACKUP_TYPE = N'''+ISNULL(@BK_TYPE, N'')+N''', @FILENAME = N'''+ISNULL(@BK_PHYSICAL_DEVICE_NAME, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@BK_COMPRESSION_FLAG, 1) AS NVARCHAR(4))+N', @FILE = '+CAST(ISNULL(@BK_POSITION, 1) AS NVARCHAR(4))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@DB_CMPT_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
    IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
    IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
    IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
    IF @SHOW_MESSAGE_FLAG = 1 PRINT @CMD
  
    --Actual execution of the remote restore command
    EXEC master..sp_executesql @CMD
   END

  FETCH NEXT FROM CUR_RES INTO @BK_FINISH_DATE, @BK_TYPE, @BK_POSITION, @BK_PHYSICAL_DEVICE_NAME
  END
  CLOSE CUR_RES
  DEALLOCATE CUR_RES
 END
  ELSE
 BEGIN
  RAISERROR(N'Invalid DRP type', 16, 1)
  RETURN
 END
END

-------------------------------------
--finish and cleanup
AbortDrp:
DROP TABLE #tblRestore