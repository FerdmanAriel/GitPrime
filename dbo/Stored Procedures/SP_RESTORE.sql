--EZMANAGE_
create PROCEDURE [dbo].[SP_RESTORE]
@DATABASE_NAME NVARCHAR(128),
@BACKUP_TYPE NVARCHAR(20),
@FILENAME NVARCHAR(1024),
@KEEPOPEN BIT,
@FILE INT = 1,
@NEW_DATABASE_NAME NVARCHAR(128) = NULL,
@NEW_DATABASE_DATA_LOC NVARCHAR(1024) = NULL,
@NEW_DATABASE_LOG_LOC NVARCHAR(1024) = NULL,
@STOPAT NVARCHAR(128) = NULL,
@SHOW_PROGRESS BIT = 0,
@VERIFY_ONLY BIT = 0,
@FILELIST_ONLY BIT = 0, 
@HEADER_ONLY BIT = 0,
@NEW_DB_SOURCE_CMPT_LEVEL INT = NULL,
@KEEPOPEN_STANDBY BIT = 0,
@USR_BLOCKSIZE INT = NULL,
@USR_BUFFERCOUNT INT = NULL,
@USR_MAXTRANSFERSIZE INT = NULL,
@SHOW_MESSAGE_FLAG BIT = 1

--drop table #tblFileList
--drop table #tblBackupInfo
-- DECLARE @DATABASE_NAME NVARCHAR(128) = N'Anunnaki_dr2'
-- DECLARE @BACKUP_TYPE NVARCHAR(20) =  N'L'
-- DECLARE @FILENAME NVARCHAR(1024) = N'\\DBASRV-DEV\D$\DBASRV-DEV$SQL2008\Anunnaki\LOG\20140311\DBASRV-DEV$SQL2008_Anunnaki_20140311_1119_40_170_LOG.sqmc'
-- DECLARE @KEEPOPEN BIT = 1 
-- DECLARE @ENCRYPTION_KEY NVARCHAR(1024) = NULL
-- DECLARE @FILE INT = 1
-- DECLARE @NEW_DATABASE_NAME NVARCHAR(128) = NULL
-- DECLARE @NEW_DATABASE_DATA_LOC NVARCHAR(1024) = NULL
-- DECLARE @NEW_DATABASE_LOG_LOC NVARCHAR(1024) = NULL
-- DECLARE @STOPAT NVARCHAR(128) = NULL
-- DECLARE @SHOW_PROGRESS BIT = 0
-- DECLARE @VERIFY_ONLY BIT = 0
-- DECLARE @FILELIST_ONLY BIT = 0 
-- DECLARE @HEADER_ONLY BIT = 0
-- DECLARE @NEW_DB_SOURCE_CMPT_LEVEL INT = 100
-- DECLARE @KEEPOPEN_STANDBY BIT = 1
-- DECLARE @USR_BLOCKSIZE INT = NULL
-- DECLARE @USR_BUFFERCOUNT INT = NULL
-- DECLARE @USR_MAXTRANSFERSIZE INT = NULL
-- DECLARE @SHOW_MESSAGE_FLAG BIT = 1


--WITH ENCRYPTION
AS
SET NOCOUNT ON 

DECLARE
@return_value INT ,
@MaxSeverity  INT ,
@Msgs NVARCHAR(4000),
@CMD_START DATETIME,
@CMD_DURATION VARCHAR(15),
@ERROR INT

IF @KEEPOPEN IS NULL SET @KEEPOPEN = 0
IF (@FILE IS NULL) OR (@FILE = 0) SET @FILE = 1
IF (@VERIFY_ONLY IS NULL) SET @VERIFY_ONLY = 0
IF (@FILELIST_ONLY IS NULL) SET @FILELIST_ONLY = 0
IF (@HEADER_ONLY IS NULL) SET @HEADER_ONLY = 0

IF @USR_BLOCKSIZE = 0 SET @USR_BLOCKSIZE = NULL
IF @USR_BLOCKSIZE IS NOT NULL IF @USR_BLOCKSIZE > 65536 SET @USR_BLOCKSIZE = NULL
IF @USR_BLOCKSIZE IS NOT NULL IF @USR_BLOCKSIZE < 2048 SET @USR_BLOCKSIZE = NULL

IF @USR_BUFFERCOUNT = 0 SET @USR_BUFFERCOUNT = NULL

IF @USR_MAXTRANSFERSIZE = 0 SET @USR_MAXTRANSFERSIZE = NULL
IF @USR_MAXTRANSFERSIZE IS NOT NULL IF @USR_MAXTRANSFERSIZE > 4194304 SET @USR_MAXTRANSFERSIZE = NULL
IF @USR_MAXTRANSFERSIZE IS NOT NULL IF @USR_MAXTRANSFERSIZE < 65536 SET @USR_MAXTRANSFERSIZE = NULL

DECLARE @CMD NVARCHAR(4000)
DECLARE @INSTANCE_NAME NVARCHAR(128)
DECLARE @BACKUP_TYPE_FILE NVARCHAR(80)
DECLARE @LOGICAL_NAME NVARCHAR(128)
DECLARE @PHYSICAL_NAME NVARCHAR(1024)
DECLARE @FILEGROUP_NAME NVARCHAR(128)
DECLARE @FILELIST_TYPE NVARCHAR(20)
DECLARE @CMD_INFO NVARCHAR(4000)
DECLARE @CMD_LIST NVARCHAR(4000)
DECLARE @RES INT
DECLARE @COUNTER INT
DECLARE @FILENAMEONLY NVARCHAR(1024)
DECLARE @NEW_LOGICAL_SUFFIX NVARCHAR(128)
DECLARE @NEW_LOGICAL_DATA_COUNTER INT
DECLARE @NEW_LOGICAL_DATA_COUNTER_SUFFIX NVARCHAR(20)
DECLARE @NEW_LOGICAL_LOG_COUNTER INT
DECLARE @NEW_LOGICAL_LOG_COUNTER_SUFFIX NVARCHAR(20)
DECLARE @STANDBY_FILENAME NVARCHAR(2400)

DECLARE @UPGRADE_NEEDED BIT
DECLARE @CUR_CMPT_LEVEL INT

SET @UPGRADE_NEEDED = 1
SET @NEW_LOGICAL_DATA_COUNTER = 0
SET @NEW_LOGICAL_LOG_COUNTER = 0
SET @NEW_LOGICAL_DATA_COUNTER_SUFFIX = N''
SET @NEW_LOGICAL_LOG_COUNTER_SUFFIX = N''
SET @STANDBY_FILENAME = N''

SELECT @BACKUP_TYPE_FILE = CASE @BACKUP_TYPE
WHEN N'D' THEN N'DATA'
WHEN N'L' THEN N'LOG'
WHEN N'I' THEN N'DIFF'
ELSE NULL
END

-- Ray Maor 11-10-2018 Checking if file is native or not (input parameter @USE_NATIVE_RESTORE might be incorrect)

declare @ProductVersion NVARCHAR(128)
declare @ProductVersionNumber TINYINT

SET @ProductVersion = CONVERT(NVARCHAR(128),SERVERPROPERTY('ProductVersion'))
SET @ProductVersionNumber = SUBSTRING(@ProductVersion, 1, (CHARINDEX('.', @ProductVersion) - 1))

if (@ProductVersionNumber >= 10)
begin try 
	RESTORE headeronly FROM DISK = @FILENAME ;
end try
begin catch
	print 'not native restore'
end catch


 --Added by Ray Maor, 03.06.2018 
 --Removing log file restores that already contain the LSN that is in the database
IF (@BACKUP_TYPE = N'L')
begin try
	-- First getting the database latest LSN
	declare @db_last_lsn numeric(30) 
	declare @file_last_lsn numeric(30) 
	
	SELECT TOP 1 @db_last_lsn  = b.last_lsn--, b.type, b.first_lsn, b.checkpoint_lsn, b.database_backup_lsn
	FROM msdb..restorehistory a
	INNER JOIN msdb..backupset b ON a.backup_set_id = b.backup_set_id
	WHERE a.destination_database_name = @DATABASE_NAME
	ORDER BY restore_date DESC

	if (@db_last_lsn  is not null)
	begin
		-- Getting the header from the log
		declare @LSN  varchar(256) 
		declare @LSNNUMERIC  numeric(30)
		
		select top 1 @LSNNUMERIC  = LSN 
			from DR_Log_SLN 
			where Database_Name = @DATABASE_NAME and [File_Name] = RIGHT(@FILENAME, CHARINDEX('\', REVERSE(@FILENAME)) -1)
		
		if (@LSNNUMERIC is not null)
			print 'found LSN in cache '+ @FILENAME
		else
		begin
			print 'could not find LSN, getting from file '+ @FILENAME
			exec EZManagePro..SP_BACKUP_GET_LSN_FROM_FILE @FILENAME , @LSN  output 
			set @LSNNUMERIC = cast(@LSN as numeric(30))
			insert into EZManagePro..DR_Log_SLN values (@DATABASE_NAME,RIGHT(@FILENAME, CHARINDEX('\', REVERSE(@FILENAME)) -1),GETDATE(), @LSNNUMERIC)
		end
			
		if (@LSNNUMERIC is not null)
		begin
			print 'db last lsn = ' +cast (@db_last_lsn as nvarchar(100)) + ' file LSN= ' + cast (@LSNNUMERIC as nvarchar(100))
			if (@LSNNUMERIC <= @db_last_lsn)
			begin
				print 'Skipping old log file: log file tblBackupHeader is ' + cast (@LSNNUMERIC as nvarchar(100))  + ', database LSN is: ' + cast (@db_last_lsn as nvarchar(100))
				return
			end
			else
				print 'not Skipping old log file: log file LSN is ' + cast (@LSNNUMERIC as nvarchar(100))  + ', database LSN is: ' + cast (@db_last_lsn as nvarchar(100))			
		end
	end
end try
begin catch
	print 'exception: ' + cast (@@Error as nvarchar(100))
	-- Just in case there was any error in catching the SLN nubmer to avoid raising the exception from the SQLVDI - we continue
end catch

print 'BACKUP_TYPE_FILE: = ' + @BACKUP_TYPE_FILE
print 'BACKUP_TYPE: = ' + @BACKUP_TYPE
IF @BACKUP_TYPE_FILE IS NULL 
BEGIN
RAISERROR(N'Backup type is invalid, please use: D/L/I only', 16, 1)
RETURN
END

IF (@BACKUP_TYPE = N'I') OR (@BACKUP_TYPE = N'L')
BEGIN
--This is a log/diff restore request, we need to make sure that the original file to restore from
--is in the same compatibility level as the destination db...
print''
print '@NEW_DB_SOURCE_CMPT_LEVEL : = ' + cast(@NEW_DB_SOURCE_CMPT_LEVEL as NVARCHAR(1200))
print ''

IF @NEW_DB_SOURCE_CMPT_LEVEL IS NOT NULL
BEGIN
  IF EXISTS (SELECT * FROM master..sysdatabases WHERE [name] = @DATABASE_NAME)
  BEGIN
   IF @NEW_DB_SOURCE_CMPT_LEVEL < (SELECT TOP 1 [cmptlevel] FROM master..sysdatabases WHERE [name] = @DATABASE_NAME)
   BEGIN
      RAISERROR(N'Unable to restore LOG/DIFF backup file to a different compatibility level database', 16, 1)
      RETURN
   END
  END
END
END

SET @INSTANCE_NAME = (SELECT CAST(SERVERPROPERTY(N'InstanceName') AS NVARCHAR(128)))
print''
print '@INSTANCE_NAME := ' + @INSTANCE_NAME
print''

print''
print '@FILENAME := ' + @FILENAME
print''

IF (@FILENAME IS NULL) OR (@FILENAME = N'')
BEGIN
RAISERROR(N'Restore file name is invalid or empty', 16, 1)
RETURN
END

print''
print '@NEW_DATABASE_NAME  := ' + cast(@NEW_DATABASE_NAME as NVARCHAR(128))
print''

print''
print '@DATABASE_NAME  := ' + cast(@DATABASE_NAME as NVARCHAR(1200))
print''

print''
print '@VERIFY_ONLY  := ' + cast(@VERIFY_ONLY as NVARCHAR(1200))
print '@FILELIST_ONLY  := ' + cast(@FILELIST_ONLY as NVARCHAR(1200))
print '@HEADER_ONLY  := ' + cast(@HEADER_ONLY as NVARCHAR(1200))
print''

IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'')
begin
print ' IN IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'')'
IF (@DATABASE_NAME IS NULL) OR (@DATABASE_NAME = N'')
BEGIN
  IF (@VERIFY_ONLY = 0) AND (@FILELIST_ONLY = 0) AND (@HEADER_ONLY = 0)
  BEGIN
   RAISERROR(N'Database name for restore is invalid or empty', 16, 1)
   RETURN
  END
END
END
ELSE
begin

print ' IN ELSE of IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'')'

IF (@NEW_DATABASE_DATA_LOC IS NULL) OR (@NEW_DATABASE_DATA_LOC = N'')
BEGIN
  EXEC @RES = master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @NEW_DATABASE_DATA_LOC OUTPUT, N'NO_OUTPUT'
  IF (@NEW_DATABASE_DATA_LOC IS NULL) OR (@NEW_DATABASE_DATA_LOC = N'')
  BEGIN
   EXEC @RES = master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\Setup', N'SQLDataRoot', @NEW_DATABASE_DATA_LOC OUTPUT, N'NO_OUTPUT'
   IF RIGHT(@NEW_DATABASE_DATA_LOC, 1) != N'\' SET @NEW_DATABASE_DATA_LOC = @NEW_DATABASE_DATA_LOC+N'\'
   SET @NEW_DATABASE_DATA_LOC = @NEW_DATABASE_DATA_LOC+N'Data'
  END
END

print ' AFTER master..xp_instance_regread'

IF (@NEW_DATABASE_LOG_LOC IS NULL) OR (@NEW_DATABASE_LOG_LOC = N'')
BEGIN
  EXEC @RES = master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @NEW_DATABASE_LOG_LOC OUTPUT, N'NO_OUTPUT'
  IF (@NEW_DATABASE_LOG_LOC IS NULL) OR (@NEW_DATABASE_LOG_LOC = N'')
  BEGIN
   EXEC @RES = master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\Setup', N'SQLDataRoot', @NEW_DATABASE_LOG_LOC OUTPUT, N'NO_OUTPUT'
   IF RIGHT(@NEW_DATABASE_LOG_LOC, 1) != N'\' SET @NEW_DATABASE_LOG_LOC = @NEW_DATABASE_LOG_LOC+N'\'
   SET @NEW_DATABASE_LOG_LOC = @NEW_DATABASE_LOG_LOC+N'Data'
  END
END

IF RIGHT(@NEW_DATABASE_DATA_LOC, 1) != N'\' SET @NEW_DATABASE_DATA_LOC = @NEW_DATABASE_DATA_LOC+N'\'
IF RIGHT(@NEW_DATABASE_LOG_LOC, 1) != N'\' SET @NEW_DATABASE_LOG_LOC = @NEW_DATABASE_LOG_LOC+N'\'
END



CREATE TABLE #tblFileList (
[LogicalName] NVARCHAR(128),
[PhysicalName] NVARCHAR(1024),
[Type] NVARCHAR(20),
[Size] NVARCHAR(20)
)

CREATE TABLE #tblBackupInfo (
[LogicalName] NVARCHAR(128),
[PhysicalName] NVARCHAR(1024),
[Type] NVARCHAR(20),
[FileGroupName] NVARCHAR(128),
[Size] NUMERIC(20, 0),
[MaxSize] NUMERIC(20, 0) 
)

IF LEFT(CONVERT(VARCHAR,SERVERPROPERTY(N'ProductVersion')),-1+CHARINDEX('.',CONVERT(VARCHAR,SERVERPROPERTY(N'ProductVersion')),1)) > 8
BEGIN
ALTER TABLE #tblBackupInfo ADD [FileId] INT NULL
ALTER TABLE #tblBackupInfo ADD [CreateLSN] NUMERIC(25, 0) NULL
ALTER TABLE #tblBackupInfo ADD [DropLSN] NUMERIC(25, 0) NULL
ALTER TABLE #tblBackupInfo ADD [UniqueId] UNIQUEIDENTIFIER NULL
ALTER TABLE #tblBackupInfo ADD [ReadOnlyLSN] NUMERIC(25, 0) NULL
ALTER TABLE #tblBackupInfo ADD [ReadWriteLSN] NUMERIC(25, 0) NULL
ALTER TABLE #tblBackupInfo ADD [BackupSizeInBytes] BIGINT NULL
ALTER TABLE #tblBackupInfo ADD [SourceBlockSize] INT NULL
ALTER TABLE #tblBackupInfo ADD [FileGroupId] INT NULL
ALTER TABLE #tblBackupInfo ADD [LogGroupGUID] UNIQUEIDENTIFIER NULL
ALTER TABLE #tblBackupInfo ADD [DifferentialBaseLSN] NUMERIC(25, 0) NULL
ALTER TABLE #tblBackupInfo ADD [DifferentialBaseGUID] UNIQUEIDENTIFIER NULL
ALTER TABLE #tblBackupInfo ADD [IsReadOnly] INT NULL
ALTER TABLE #tblBackupInfo ADD [IsPresent] INT NULL
ALTER TABLE #tblBackupInfo ADD [TDEThumbprint] INT NULL
END
IF LEFT(CONVERT(VARCHAR,SERVERPROPERTY(N'ProductVersion')),-1+CHARINDEX('.',CONVERT(VARCHAR,SERVERPROPERTY(N'ProductVersion')),1)) > 12
BEGIN
ALTER TABLE #tblBackupInfo ADD [SnapshotUrl]  NVARCHAR(360)
END
SET @FILENAMEONLY = REVERSE(@FILENAME)
print''
print '@FILENAMEONLY : = ' + @FILENAMEONLY
print''

IF CHARINDEX(N'\', @FILENAMEONLY) > 0
BEGIN
SET @FILENAMEONLY = SUBSTRING(@FILENAMEONLY, 0, CHARINDEX(N'\', @FILENAMEONLY))
SET @FILENAMEONLY = REVERSE(@FILENAMEONLY)
END

print '@FILENAMEONLY : = ' + @FILENAMEONLY

IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'')
begin
IF (@DATABASE_NAME IS NULL) OR (@DATABASE_NAME = N'')
  BEGIN
  IF @VERIFY_ONLY = 1
  BEGIN
   SET @CMD = N'RESTORE VERIFYONLY FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(80))
  END
   ELSE
  BEGIN
   IF @FILELIST_ONLY = 1
   BEGIN
      SET @CMD = N'RESTORE FILELISTONLY FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(80))
   END
      ELSE
   BEGIN
      IF @HEADER_ONLY = 1
      BEGIN
      SET @CMD = N'RESTORE HEADERONLY FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(80))
      END
      ELSE
      BEGIN
      RAISERROR(N'Invalid restore operation', 16, 1)
      RETURN
      END
   END
  END
END
  ELSE
BEGIN
  SELECT @CMD = CASE @BACKUP_TYPE 
   WHEN N'D' THEN N'RESTORE DATABASE ['+@DATABASE_NAME+'] FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))+N', REPLACE' 
   WHEN N'L' THEN N'RESTORE LOG ['+@DATABASE_NAME+'] FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))+N', REPLACE'
   WHEN N'I' THEN N'RESTORE DATABASE ['+@DATABASE_NAME+'] FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))+N', REPLACE'
  END
END
END
ELSE
BEGIN
SELECT @CMD = CASE @BACKUP_TYPE 
  WHEN N'D' THEN N'RESTORE DATABASE ['+@NEW_DATABASE_NAME+'] FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))+N', REPLACE' 
  WHEN N'L' THEN N'RESTORE LOG ['+@NEW_DATABASE_NAME+'] FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))+N', REPLACE'
  WHEN N'I' THEN N'RESTORE DATABASE ['+@NEW_DATABASE_NAME+'] FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))+N', REPLACE' 
 END

SET @CMD_INFO = N'RESTORE FILELISTONLY FROM DISK = N'''+@FILENAME+''' WITH FILE = '+CAST(@FILE AS NVARCHAR(20))
IF @SHOW_MESSAGE_FLAG = 1 
PRINT '1:'+@CMD_INFO
INSERT INTO #tblBackupInfo EXEC master..sp_executesql @CMD_INFO

DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
FOR
  SELECT LogicalName, PhysicalName, FileGroupName FROM #tblBackupInfo
OPEN CUR
FETCH NEXT FROM CUR INTO @LOGICAL_NAME, @PHYSICAL_NAME, @FILEGROUP_NAME
WHILE @@FETCH_STATUS = 0
BEGIN

  IF CHARINDEX(N'.', SUBSTRING(REVERSE(@PHYSICAL_NAME), 0, CHARINDEX(N'\', REVERSE(@PHYSICAL_NAME)))) > 0
   SET @NEW_LOGICAL_SUFFIX = REVERSE(SUBSTRING(SUBSTRING(REVERSE(@PHYSICAL_NAME), 0, CHARINDEX(N'\', REVERSE(@PHYSICAL_NAME))), 0, CHARINDEX(N'.', SUBSTRING(REVERSE(@PHYSICAL_NAME), 0, CHARINDEX(N'\', REVERSE(@PHYSICAL_NAME))))+1))
  ELSE
   SET @NEW_LOGICAL_SUFFIX = N''

  IF @FILEGROUP_NAME IS NULL
  BEGIN
   SET @NEW_LOGICAL_LOG_COUNTER = @NEW_LOGICAL_LOG_COUNTER + 1
   IF @NEW_LOGICAL_LOG_COUNTER > 1 SET @NEW_LOGICAL_LOG_COUNTER_SUFFIX = CAST((@NEW_LOGICAL_LOG_COUNTER - 1) AS INT)
   SET @CMD = @CMD+N', MOVE N'''+@LOGICAL_NAME+''' TO N'''+@NEW_DATABASE_LOG_LOC+@NEW_DATABASE_NAME+@NEW_LOGICAL_LOG_COUNTER_SUFFIX+@NEW_LOGICAL_SUFFIX+''''
  END
   ELSE
  BEGIN
   SET @NEW_LOGICAL_DATA_COUNTER = @NEW_LOGICAL_DATA_COUNTER + 1
   IF @NEW_LOGICAL_DATA_COUNTER > 1 SET @NEW_LOGICAL_DATA_COUNTER_SUFFIX = CAST((@NEW_LOGICAL_DATA_COUNTER - 1) AS INT)
   SET @CMD = @CMD+N', MOVE N'''+@LOGICAL_NAME+''' TO N'''+@NEW_DATABASE_DATA_LOC+@NEW_DATABASE_NAME+@NEW_LOGICAL_DATA_COUNTER_SUFFIX+@NEW_LOGICAL_SUFFIX+''''
  END

FETCH NEXT FROM CUR INTO @LOGICAL_NAME, @PHYSICAL_NAME, @FILEGROUP_NAME
END

CLOSE CUR
DEALLOCATE CUR
END

print 'StartRestore:' 

IF @VERIFY_ONLY = 0 AND @FILELIST_ONLY = 0 AND @HEADER_ONLY = 0
BEGIN

print' ' 
print 'SHOW_PROGRESS := ' + cast(@SHOW_PROGRESS as NVARCHAR(1000))
print ' '
IF @SHOW_PROGRESS = 1 SET @CMD = @CMD+N', STATS = 1'
--*************** Remove 6/7/2011

print' ' 
print '@NEW_DB_SOURCE_CMPT_LEVEL := ' + cast(@NEW_DB_SOURCE_CMPT_LEVEL as NVARCHAR(1000))
print ' '

IF @NEW_DB_SOURCE_CMPT_LEVEL IS NOT NULL
begin
  --This means that the database doesnt exist, we need to check according to the backup file compatibility
  --level comparing to the master's compatibility level.
  SELECT TOP 1 @CUR_CMPT_LEVEL = [cmptlevel] FROM master..sysdatabases WHERE [name] = N'master'
  
  print '@CUR_CMPT_LEVEL := ' + cast(@CUR_CMPT_LEVEL as varchar(20))
  
  --*************** Remove 6/7/2011 todo avi fix
  IF @NEW_DB_SOURCE_CMPT_LEVEL < @CUR_CMPT_LEVEL SET @UPGRADE_NEEDED = 1
END
  ELSE
begin
  IF (@DATABASE_NAME IS NULL) OR (@DATABASE_NAME = N'') SET @DATABASE_NAME = @NEW_DATABASE_NAME
END

print ' '
print '@KEEPOPEN : = ' + cast (@KEEPOPEN as varchar) 
print' '

IF @KEEPOPEN = 1
      BEGIN
            IF @KEEPOPEN_STANDBY = 1
                begin
                        print ' @KEEPOPEN_STANDBY = 1'
                        SET @STANDBY_FILENAME = REPLACE(CAST(SERVERPROPERTY(N'ServerName') AS NVARCHAR(128)), N'\', N'$')+N'_'
                        print ''
                        IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'')
                                BEGIN                                                    
                                        print ' THE (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'') '
                                        SET @STANDBY_FILENAME = @STANDBY_FILENAME+@DATABASE_NAME+N'_'                 
                                END         
                        ELSE
                                BEGIN
                                        print ' THE (@NEW_DATABASE_NAME IS NOT NULL) OR (@NEW_DATABASE_NAME <> N'') '
                                        SET @STANDBY_FILENAME = @STANDBY_FILENAME+@NEW_DATABASE_NAME+N'_'
                                END
                                 
                        SET @STANDBY_FILENAME = @STANDBY_FILENAME+CONVERT(NVARCHAR(30), GETDATE(), 112)+N'_'+LEFT(REPLACE(CONVERT(NVARCHAR(80), GETDATE(), 108), N':', N''), 4)+N'_'+@BACKUP_TYPE
                        SET @STANDBY_FILENAME = REVERSE(RIGHT(REVERSE(@FILENAME), LEN(@FILENAME) - CHARINDEX(N'\', REVERSE(@FILENAME)) + 1))+@STANDBY_FILENAME
                        IF @UPGRADE_NEEDED = 0 SET @CMD = @CMD+N', STANDBY = N'''+@STANDBY_FILENAME+N'.standby'''
                        print '@@FILENAME : = ' + cast (@FILENAME as NVARCHAR(1200))
                        print '@STANDBY_FILENAME : = ' + cast (@STANDBY_FILENAME as NVARCHAR(1200))
                        print '@UPGRADE_NEEDED : = ' + cast (@UPGRADE_NEEDED as NVARCHAR(1200))
                END
            ELSE 
            BEGIN
            --Keep open - but not STANDBY (for log restore...)
                print ''
                print ' WITH NORECOVERY ' 
                print ''
                SET @CMD = @CMD+N', NORECOVERY'
            END
      end
ELSE
    BEGIN
        print ' WITH RECOVERY ' 
        --Database shouldnt stay open
        SET @CMD = @CMD+N', RECOVERY'
    END

print 'STOPAT := ' + cast(@STOPAT as NVARCHAR(1200))

IF (@STOPAT IS NOT NULL) AND (@STOPAT != N'') 
	SET @CMD = @CMD+N', STOPAT = N'''+@STOPAT+''''
END
ELSE
BEGIN
	IF @VERIFY_ONLY = 1 SET @CMD = @CMD+N', STATS = 1'
END

print ' Setting optional advanced backup parameters...'
--Setting optional advanced backup parameters...
IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', BLOCKSIZE = '+CAST(@USR_BLOCKSIZE AS NVARCHAR(20))
IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', BUFFERCOUNT = '+CAST(@USR_BUFFERCOUNT AS NVARCHAR(20))
IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', MAXTRANSFERSIZE = '+CAST(@USR_MAXTRANSFERSIZE AS NVARCHAR(20))

print ' @CMD := ' + cast(@CMD as NVARCHAR(3000))

IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'')
begin
IF EXISTS (SELECT sys.[spid] FROM master..sysprocesses sys INNER JOIN master..sysdatabases sdb ON sys.dbid = sdb.dbid WHERE sdb.[name] = @DATABASE_NAME)
BEGIN
  PRINT(N' - Killing active connections on database "'+@DATABASE_NAME+N'"')
  DECLARE @SPID INT
  DECLARE @KILL_CMD NVARCHAR(512)
  SET @KILL_CMD='alter database [' + @DATABASE_NAME +'] set single_user with rollback immediate'
  IF @SHOW_MESSAGE_FLAG = 1 
      PRINT @KILL_CMD
  EXEC master..sp_executesql @KILL_CMD
  SET @KILL_CMD='alter database [' + @DATABASE_NAME +'] set multi_user '
  EXEC master..sp_executesql @KILL_CMD
END
END

PRINT(N' - Starting restore from "'+@FILENAME+'"')
IF @SHOW_MESSAGE_FLAG = 1 
      PRINT @CMD

SET @CMD_START=GETDATE()
EXEC master..sp_executesql @CMD 
return
SET @ERROR=@@ERROR
IF @ERROR<>0
   BEGIN 
   SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
   RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
           16, -- Severity,
           1, -- State,
           @CMD, -- First argument.
               @CMD_DURATION,
           @ERROR) WITH LOG 
      RETURN          
   END 

DROP TABLE #tblBackupInfo
DROP TABLE #tblFileList

IF @UPGRADE_NEEDED = 1
BEGIN
--The upgrade is done using the sp_dbcmptlevel procedure, but this procedure cannot
--run from inside a stored procedure so we'll run it from a cmdshell execution
--Before we do - we need to see that the cmdshell is open, and if not - open it and
--close it when we finish.

IF (@KEEPOPEN = 1 AND @KEEPOPEN_STANDBY = 1) OR (@KEEPOPEN = 0)
BEGIN

  --This means, if the database is not to be kept open or if it is kept open in standby mode
  --then we verify if it needs to be upgraded

  IF (@NEW_DATABASE_NAME IS NULL) OR (@NEW_DATABASE_NAME = N'') SET @NEW_DATABASE_NAME = @DATABASE_NAME

  PRINT(N' - Upgrading database ['+@NEW_DATABASE_NAME+N'] to compatibility level '+CAST(@CUR_CMPT_LEVEL AS NVARCHAR(20)))
  DECLARE @ADVANCED_OPTIONS_VALUE INT
  DECLARE @CMD_SHELL_VALUE INT
  DECLARE @CMD_COMMAND NVARCHAR(1200)
  SET @CMD_COMMAND = N'osql -S"'+CAST(SERVERPROPERTY(N'ServerName') AS NVARCHAR(128))+N'" -E -Q"EXEC master..sp_dbcmptlevel N'''+@NEW_DATABASE_NAME+N''', '+CAST(@CUR_CMPT_LEVEL AS NVARCHAR(20))+N'"'
  IF CAST(SUBSTRING(CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(80)), 0, CHARINDEX(N'.', CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(80)))) AS INT) > 8
  BEGIN
   CREATE TABLE #tblCmdShell (
      [name] nvarchar(35),
      [minimum] int,
      [maximum] int,
      [config_value] int,
      [run_value] int
   )
   INSERT INTO #tblCmdShell EXEC master..sp_configure N'show advanced options'
   SELECT TOP 1 @ADVANCED_OPTIONS_VALUE = config_value FROM #tblCmdShell
   DELETE #tblCmdShell

   IF @ADVANCED_OPTIONS_VALUE = 0
   BEGIN
      EXEC master..sp_configure N'show advanced options', 1
      RECONFIGURE WITH OVERRIDE
   END

   INSERT INTO #tblCmdShell EXEC master..sp_configure N'xp_cmdshell'
   SELECT TOP 1 @CMD_SHELL_VALUE = config_value FROM #tblCmdShell

   IF @CMD_SHELL_VALUE = 0
   BEGIN
      EXEC master..sp_configure N'xp_cmdshell', 1
      RECONFIGURE WITH OVERRIDE
   END
  END

print '@CMD_COMMAND : = ' + @CMD_COMMAND
  EXEC master..xp_cmdshell @CMD_COMMAND, NO_OUTPUT

  IF CAST(SUBSTRING(CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(80)), 0, CHARINDEX(N'.', CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(80)))) AS INT) > 8
  BEGIN
   IF @CMD_SHELL_VALUE = 0
   BEGIN
      EXEC master..sp_configure N'xp_cmdshell', 0
      RECONFIGURE WITH OVERRIDE
   END

   IF @ADVANCED_OPTIONS_VALUE = 0
   BEGIN
      EXEC master..sp_configure N'show advanced options', 0
      RECONFIGURE WITH OVERRIDE
   END
   DROP TABLE #tblCmdShell
  END

  SET @CMD = N'EXEC ['+@NEW_DATABASE_NAME+N']..sp_updatestats'
  IF @SHOW_MESSAGE_FLAG = 1 
      PRINT @CMD
      
  EXEC master..sp_executesql @CMD
  


  IF @KEEPOPEN = 1
  BEGIN
   IF @KEEPOPEN_STANDBY = 1
   BEGIN
      --This means that the database was supposed to be in STANDBY mode, but because it was upgraded
      --STANDBY is not supported, what we'll do is we'll keep the database in read-only mode.
      SET @CMD = N'ALTER DATABASE ['+@NEW_DATABASE_NAME+N'] SET READ_ONLY'
      IF @SHOW_MESSAGE_FLAG = 1 
            PRINT @CMD
            
    
      EXEC master..sp_executesql @CMD
      
   END
  END
END
END 
-- test
--exec [SP_RESTORE] 'bignw_test01','D','d:/ray/DEVSRV$SQL2019_bignw_test01_DATA.bak', 
--0,1,'bignw_test02','D:\Ray\DATA','D:\Ray\LOG'