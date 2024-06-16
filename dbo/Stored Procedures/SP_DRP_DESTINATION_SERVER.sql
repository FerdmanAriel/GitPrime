--EZMANAGE_
CREATE PROCEDURE [dbo].[SP_DRP_DESTINATION_SERVER]
-- Old name: EZMPS_DRP_DESTINATION_SERVER

@SOURCE_SERVERNAME NVARCHAR(128),
@SOURCE_DATABASE NVARCHAR(128),
@DRP_TYPE NVARCHAR(80),
@SOURCE_DATABASE_COMPATIBILITY_LEVEL INT,
@BACKUP_FOLDER_DATA NVARCHAR(1200),
@BACKUP_FOLDER_LOG NVARCHAR(1200),
@BACKUP_FOLDER_DIFF NVARCHAR(1200),
@DESTINATION_DATABASE_NAME_SUFFIX NVARCHAR(128), 
@USR_BLOCKSIZE INT = NULL,
@USR_BUFFERCOUNT INT = NULL,
@USR_MAXTRANSFERSIZE INT = NULL,
@SHOW_MESSAGE_FLAG BIT = 0,
@IGNORE_FILE_PATTERN NVARCHAR(80) = N'.tmp',
@FORCE_DATABASE_RESTORE BIT=0,
@AUTO_RECOVERY BIT=1,
@SOURCE_SQL_VERSION NVARCHAR(6)='10.0' -- Now is a Dinamic Parameter!!
--WITH ENCRYPTION
AS

SET NOCOUNT ON 


-- Add By Israel Eitan Pro on 13/03/2014, Retrive the right @BACKUP_FOLDER_DATA--------------------------------------
print 'before reverse backup folder : ' + @BACKUP_FOLDER_DATA
declare @MainDataFolder varchar(1200) 
declare @ReverseBackupFolder as NVARCHAR(1200) 
set @ReverseBackupFolder = reverse(@BACKUP_FOLDER_DATA)
print @ReverseBackupFolder
declare @USE_NATIVE_RESTORE bit 
set @USE_NATIVE_RESTORE  = 0


SET @ReverseBackupFolder = reverse(substring(@ReverseBackupFolder,2,8))
print @ReverseBackupFolder
set @ReverseBackupFolder = SUBSTRING(@ReverseBackupFolder,0,3 )  + '/' +  SUBSTRING(@ReverseBackupFolder,3,2 ) + '/' + SUBSTRING (@ReverseBackupFolder,5,4)

declare @Is_Contains_Date bit 
set @Is_Contains_Date = 0

begin try
declare @testDate as DATETIME
set @testDate = convert(DATETIME, @ReverseBackupFolder, 103)
end try
begin catch
set @Is_Contains_Date = 1
end catch

print 'contains date? : ' + cast (@Is_Contains_Date as NVARCHAR(10))

IF ( @Is_Contains_Date = 0 ) 
BEGIN
	print '@BACKUP_FOLDER_DATA Before: ' + @BACKUP_FOLDER_DATA
	set @MainDataFolder = substring(@BACKUP_FOLDER_DATA,0,CHARINDEX ('DATA',@BACKUP_FOLDER_DATA,0) + 4)

	CREATE TABLE #tblMainDataDir (
	[subdirectory] NVARCHAR(1200),
	[depth] INT,
	[file] INT
	)

	INSERT INTO #tblMainDataDir EXEC master..xp_dirtree @MainDataFolder,1,1

	select * from #tblMainDataDir
	print '@MainDataFolder = ' + @MainDataFolder
	select top 1 @BACKUP_FOLDER_DATA = @MainDataFolder + '\' +[subdirectory] + '\'  from #tblMainDataDir order by [subdirectory] asc
	print '@BACKUP_FOLDER_DATA After: ' + @BACKUP_FOLDER_DATA
END

--------------------------------------------------------------------------------------------------------------------
--------Yehoda Lasri Change-----------------------------------------------------------------------------------------
DECLARE @DESTINATION_SQL_VERSION NVARCHAR(6)
DECLARE @PRODUCT sysname
SELECT  @PRODUCT =CONVERT(sysname,SERVERPROPERTY ('ProductVersion'))
print '@@PRODUCT: ' + cast(@PRODUCT as NVARCHAR(1200))

SELECT @DESTINATION_SQL_VERSION  = SUBSTRING (@PRODUCT,0,CHARINDEX('.',@PRODUCT,4))
print '@DESTINATION_SQL_VERSION : ' + @DESTINATION_SQL_VERSION

--------------------------------------------------------------------------------------------------------------------
--------Israel Eitan Pro Change---01.04.2014------------------------------------------------------------------------
DECLARE @SOURCE_VERSION NVARCHAR(1200)
print '@SOURCE_VERSION :' + @SOURCE_VERSION

SELECT @SOURCE_VERSION =SUBSTRING (@SOURCE_SQL_VERSION,0,CHARINDEX('.',@SOURCE_SQL_VERSION,4))
--------------------------------------------------------------------------------------------------------------------
print '@SOURCE_SQL_VERSION: ' + cast(@SOURCE_VERSION as NVARCHAR(1200))
print '@DESTINATION_SQL_VERSION: ' + cast(@DESTINATION_SQL_VERSION as NVARCHAR(1200))

DECLARE @DO_NOT_RUN_BACKUP BIT,@ERROR INT,@CMD_START DATETIME,@CMD_DURATION VARCHAR(15),@LOG_LAST_RUN_STATUS INT
SET @DO_NOT_RUN_BACKUP=0

IF @USR_BLOCKSIZE = 0 SET @USR_BLOCKSIZE = NULL
IF @USR_BLOCKSIZE IS NOT NULL IF @USR_BLOCKSIZE > 65536 SET @USR_BLOCKSIZE = NULL
IF @USR_BLOCKSIZE IS NOT NULL IF @USR_BLOCKSIZE < 2048 SET @USR_BLOCKSIZE = NULL

IF @USR_BUFFERCOUNT = 0 SET @USR_BUFFERCOUNT = NULL

IF @USR_MAXTRANSFERSIZE = 0 SET @USR_MAXTRANSFERSIZE = NULL
IF @USR_MAXTRANSFERSIZE IS NOT NULL IF @USR_MAXTRANSFERSIZE > 4194304 SET @USR_MAXTRANSFERSIZE = NULL
IF @USR_MAXTRANSFERSIZE IS NOT NULL IF @USR_MAXTRANSFERSIZE < 65536 SET @USR_MAXTRANSFERSIZE = NULL

DECLARE @CMD NVARCHAR(3200)
DECLARE @DESTINATION_DB_NAME NVARCHAR(256)
DECLARE @LAST_RESTORE_DATE INT
DECLARE @LAST_RESTORE_TIME INT

DECLARE @LAST_DATA_BACKUP_FILE NVARCHAR(2048)
DECLARE @LAST_DATA_BACKUP_DATE INT
DECLARE @LAST_DATA_BACKUP_TIME INT
DECLARE @LAST_DATA_BACKUP_COMPRESSED_FLAG BIT

DECLARE @LAST_DIFF_BACKUP_FILE NVARCHAR(2048)
DECLARE @LAST_DIFF_BACKUP_DATE INT
DECLARE @LAST_DIFF_BACKUP_TIME INT
DECLARE @LAST_DIFF_BACKUP_COMPRESSED_FLAG BIT

DECLARE @LOG_BACKUP_FILE NVARCHAR(2048)
DECLARE @LOG_BACKUP_DATE INT
DECLARE @LOG_BACKUP_TIME INT
DECLARE @LOG_BACKUP_COMPRESSED_FLAG BIT

DECLARE @FILE_ACCESS_CHECK INT
DECLARE @ERR_MSG NVARCHAR(1200)
DECLARE @RESTORE_WITH_STANDBY_FLAG BIT

DECLARE @LOG_BACKUP_LIST_DATE INT
DECLARE @LOG_BACKUP_LIST_TIME INT

------------------------------------------------
IF @DESTINATION_DATABASE_NAME_SUFFIX IS NULL SET @DESTINATION_DATABASE_NAME_SUFFIX = N''
SET @DESTINATION_DB_NAME = @SOURCE_DATABASE+@DESTINATION_DATABASE_NAME_SUFFIX

IF @AUTO_RECOVERY=1 AND EXISTS (SELECT * FROM master..sysdatabases WHERE name=@DESTINATION_DB_NAME AND status =32)
   BEGIN 
	IF @SHOW_MESSAGE_FLAG = 1 
		 BEGIN
		   PRINT 'DROP DATABASE [' + @DESTINATION_DB_NAME  +']'
	   END 
   EXEC sp_MSkilldb @DESTINATION_DB_NAME
   END 

IF (@BACKUP_FOLDER_DATA IS NULL) OR (@BACKUP_FOLDER_DATA = N'')
BEGIN
RAISERROR(N'DATA backup path is invalid or empty', 16, 1)
RETURN
END

IF @DRP_TYPE = N'LOG' IF (@BACKUP_FOLDER_LOG IS NULL) OR (@BACKUP_FOLDER_LOG = N'')
BEGIN
RAISERROR(N'LOG backup path is invalid or empty', 16, 1)
RETURN
END

IF @DRP_TYPE = N'DIFF' IF (@BACKUP_FOLDER_DIFF IS NULL) OR (@BACKUP_FOLDER_DIFF = N'')
BEGIN
RAISERROR(N'DIFF backup path is invalid or empty', 16, 1)
RETURN
END


IF RIGHT(@BACKUP_FOLDER_DATA, 1) != N'\' SET @BACKUP_FOLDER_DATA = @BACKUP_FOLDER_DATA + N'\'
IF (@BACKUP_FOLDER_DIFF IS NOT NULL) AND (@BACKUP_FOLDER_DIFF != N'') IF RIGHT(@BACKUP_FOLDER_DIFF, 1) != N'\' SET @BACKUP_FOLDER_DIFF = @BACKUP_FOLDER_DIFF + N'\'
IF (@BACKUP_FOLDER_LOG IS NOT NULL) AND (@BACKUP_FOLDER_LOG != N'') IF RIGHT(@BACKUP_FOLDER_LOG, 1) != N'\' SET @BACKUP_FOLDER_LOG = @BACKUP_FOLDER_LOG + N'\'

--This is a temporary table that will hold the backup files from the required directories...
CREATE TABLE #tblDir (
[subdirectory] NVARCHAR(1200),
[depth] INT,
[file] INT
)

--Here we populate the backup files from the directories, according to the requested 
IF @SHOW_MESSAGE_FLAG = 1 
BEGIN
PRINT (N' (i) Populating temporary table for DATA files...')
PRINT (N'   DATA folder: "'+ISNULL(@BACKUP_FOLDER_DATA, N'')+N'"')
PRINT (N'')
END
INSERT INTO #tblDir EXEC master..xp_dirtree @BACKUP_FOLDER_DATA, 1, 1

SELECT * FROM #tblDir
IF @AUTO_RECOVERY=1
SET @LOG_LAST_RUN_STATUS=(
							  SELECT TOP 1 jh.run_status  
							  FROM msdb.dbo.sysjobhistory jh INNER JOIN  msdb.dbo.sysjobsteps j 
									ON  j.job_id=jh.job_id  AND j.step_id=jh.step_id  
							  WHERE j.command LIKE '%SP_DRP_DESTINATION_SERVER%' + @SOURCE_SERVERNAME + '%' +  @SOURCE_DATABASE + '%' + @DRP_TYPE + '%' + ISNULL(@DESTINATION_DATABASE_NAME_SUFFIX,'') + '%'
							  ORDER BY run_date DESC,run_time DESC
				 )


print ' @LOG_LAST_RUN_STATUS : ' + cast(@LOG_LAST_RUN_STATUS as NVARCHAR(1200))

print ' @@AUTO_RECOVERY : ' + cast(@AUTO_RECOVERY as NVARCHAR(1200))

IF @AUTO_RECOVERY=1 AND @LOG_LAST_RUN_STATUS=0 AND EXISTS (SELECT * FROM master..sysdatabases WHERE name=@DESTINATION_DB_NAME)
   BEGIN 

  
   SELECT TOP 1 
	@LOG_BACKUP_LIST_DATE = CAST(CONVERT(NVARCHAR(80), bkset.backup_finish_date, 112) AS INT),
	@LOG_BACKUP_LIST_TIME = CAST(LEFT(REPLACE(CONVERT(NVARCHAR(80), bkset.backup_finish_date, 108), N':', N''), 4) AS INT)
   FROM 
	msdb..restorehistory res
   INNER JOIN
	msdb..backupset bkset
	  ON res.backup_set_id = bkset.backup_set_id
   WHERE 
	destination_database_name = @DESTINATION_DB_NAME 
   ORDER BY
	res.restore_date DESC
   
   print ' @LOG_BACKUP_LIST_DATE ' + cast(@LOG_BACKUP_LIST_DATE as NVARCHAR (1200))
   print ' @@LOG_BACKUP_LIST_TIME ' + cast(@LOG_BACKUP_LIST_TIME as NVARCHAR (1200))
				  
   IF EXISTS (
  
			   SELECT * FROM #tblDir 
			   WHERE 
							  [file] = 1
							  AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
							  AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
							  AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
							  AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
							  AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LOG_BACKUP_LIST_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80))))+CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80)) AS BIGINT)
							  AND CHARINDEX('_DATA.',[subdirectory],1)>0
							  AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
				  )
				  BEGIN 
				  print 'EXIST'
				  print '@BACKUP_FOLDER_DATA : ' + @BACKUP_FOLDER_DATA
				  print '@BACKUP_FOLDER_LOG  : ' + @BACKUP_FOLDER_LOG
				  print '@BACKUP_FOLDER_DIFF : ' + @BACKUP_FOLDER_DIFF
				  
				  EXEC sp_MSkilldb @DESTINATION_DB_NAME

				  EXEC [SP_DRP_DESTINATION_SERVER]
						@SOURCE_SERVERNAME =@SOURCE_SERVERNAME,
						  @SOURCE_DATABASE=@SOURCE_DATABASE,
				  @DRP_TYPE =N'DATA',
				  @SOURCE_DATABASE_COMPATIBILITY_LEVEL=@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 
				  @BACKUP_FOLDER_DATA =@BACKUP_FOLDER_DATA,
				  @BACKUP_FOLDER_LOG=@BACKUP_FOLDER_LOG,
				  @BACKUP_FOLDER_DIFF =@BACKUP_FOLDER_DIFF,
				  @DESTINATION_DATABASE_NAME_SUFFIX =@DESTINATION_DATABASE_NAME_SUFFIX,
				  @USR_BLOCKSIZE =@USR_BLOCKSIZE,
				  @USR_BUFFERCOUNT =@USR_BUFFERCOUNT,
				  @USR_MAXTRANSFERSIZE=@USR_MAXTRANSFERSIZE,
				  @SHOW_MESSAGE_FLAG =@SHOW_MESSAGE_FLAG, 
				  @FORCE_DATABASE_RESTORE =1
		   RETURN       
				  END      
	
	
   END 
			
print '@DRP_TYPE:= ' + @DRP_TYPE

IF @DRP_TYPE = N'LOG' 

BEGIN
---------Added By Israel Eitan Pro on the 13/03/2014---Take care of Log Drp Type in case there is a Date subfolder------------  
		IF ( @Is_Contains_Date = 0) -- With a Date Sub Folder
		BEGIN
			  PRINT ''
			print 'The BACKUP_FOLDER_LOG is with a DATE'
			Print ''
			 
			 
			  declare @MainLogDirectory Nvarchar(1200)
			  set @MainLogDirectory = SUBSTRING (@BACKUP_FOLDER_LOG,0,CHARINDEX ('LOG',@BACKUP_FOLDER_LOG,0) + 3)
			
			  print 'Before LOOP'
			  Print '@MainLogDirectory :  ' + @MainLogDirectory
			
			  CREATE TABLE #tblLogMain (
			  [subdirectory] NVARCHAR(1200),
			  [depth] INT,
			  [file] INT
			  )
			  INSERT INTO #tblLogMain EXEC master..xp_dirtree @MainLogDirectory, 1, 1
			  declare @Sub NVARCHAR(1200),@Depth_log int, @file_log int

			  DECLARE CUR CURSOR FOR Select * from #tblLogMain
			  OPEN CUR
			  FETCH NEXT FROM CUR
			  INTO @Sub, @Depth_log, @file_log
			
			  print 'Before WHile'
			  select * from #tblLogMain
			
			  WHILE @@FETCH_STATUS = 0
						BEGIN
						print '*inLoop'
						print ' BACKUP_FOLDER_LOG : ' + @MainLogDirectory
						IF @BACKUP_FOLDER_LOG IS NOT NULL 
								 BEGIN
									IF @BACKUP_FOLDER_LOG != N'' 
											BEGIN
											 Declare @Temp Nvarchar(1200) 
set @Temp = @MainLogDirectory 
											 SET @Temp = @MainLogDirectory  + N'\' + @Sub
											 print '@Temp : ' + @Temp
											 IF @SHOW_MESSAGE_FLAG = 1 
													   BEGIN
															PRINT (N' (i) Populating temporary table for LOG files...')
															PRINT (N'   LOG folder: "'+ISNULL(@Temp, N'')+N'"')
															PRINT (N'')
															END
											 END   
									 INSERT INTO #tblDir EXEC master..xp_dirtree @Temp, 1, 1
									END
									FETCH NEXT FROM CUR INTO @Sub, @Depth_log, @file_log
					 END
				  close CUR
				  DEALLOCATE CUR
		END
		else -- Without a Date Sub Folder
		BEGIN
			   PRINT ''
			 print 'The BACKUP_FOLDER_LOG is without a DATE'
			 Print ''
			 
			   IF @BACKUP_FOLDER_LOG IS NOT NULL 
				   BEGIN
					IF @BACKUP_FOLDER_LOG != N'' 
					BEGIN
					 IF @SHOW_MESSAGE_FLAG = 1 
					 BEGIN
					   PRINT (N' (i) Populating temporary table for LOG files...')
						PRINT (N'   LOG folder: "'+ISNULL(@BACKUP_FOLDER_LOG, N'')+N'"')
						PRINT (N'')
					 END   
					 INSERT INTO #tblDir EXEC master..xp_dirtree @BACKUP_FOLDER_LOG, 1, 1
					END
				  END
		 END
			  
select * from #tblDir
END
----------------------------------------------------------------------------------------------------------------------------------------------

---------Added By Israel Eitan Pro on the 13/03/2014---Take care of DIFf Drp Type in case there is a Date subfolder------------  --

print 'Is_Contains_Date: = ' +  cast(@Is_Contains_Date as NVARCHAR(1200))
IF @DRP_TYPE = N'DIFF'
BEGIN
		IF ( @Is_Contains_Date = 0) -- With a Date Sub Folder
		BEGIN
			  PRINT ''
			print 'The BACKUP_FOLDER_DIFF is with a DATE'
			Print ''
					
			  declare @MainDIFFDirectory Nvarchar(1200)
			  set @MainDIFFDirectory = SUBSTRING (@BACKUP_FOLDER_DIFF,0,CHARINDEX ('DIFF',@BACKUP_FOLDER_DIFF,0) + 4)
			
			  print 'Before LOOP'
			  Print '@MainDIFFDirectory :  ' + cast(@MainDIFFDirectory as NVARCHAR(1200))
			
			  CREATE TABLE #tblDIFFMain (
			  [subdirectory] NVARCHAR(1200),
			  [depth] INT,
			  [file] INT
			  )
			  INSERT INTO #tblDIFFMain EXEC master..xp_dirtree @MainDIFFDirectory, 1, 1
			  declare @SubDIFF NVARCHAR(1200),@Depth_DIFF int, @file_DIFF int

			  DECLARE CUR CURSOR FOR Select * from #tblDIFFMain
			  OPEN CUR
			  FETCH NEXT FROM CUR
			  INTO @SubDIFF, @Depth_DIFF, @file_DIFF
			
			  print 'Before WHile'
			  select * from #tblDIFFMain
			
			  WHILE @@FETCH_STATUS = 0
						BEGIN
						print '*inLoop'
						print ' BACKUP_FOLDER_DIFF : ' + @MainDIFFDirectory
						IF @BACKUP_FOLDER_DIFF IS NOT NULL 
								 BEGIN
									IF @BACKUP_FOLDER_DIFF != N'' 
											BEGIN
											 Declare @TempDIFF Nvarchar(1200) 
set @TempDIFF = @MainDIFFDirectory 
											 SET @TempDIFF = @MainDIFFDirectory  + N'\' + @SubDIFF
											 print '@Temp : ' + @TempDIFF
											 IF @SHOW_MESSAGE_FLAG = 1 
													   BEGIN
															PRINT (N' (i) Populating temporary table for DIFF files...')
															PRINT (N'   DIFF folder: "'+ISNULL(@TempDIFF, N'')+N'"')
															PRINT (N'')
															END
											 END   
											 
											 
										  print  '@@file_DIFF := ' + cast(@file_DIFF as NVARCHAR(1200))
										  

										  
										  
									 
										 INSERT INTO #tblDir EXEC master..xp_dirtree @TempDIFF, 1, 1  
										
										  
									END
									FETCH NEXT FROM CUR INTO @SubDIFF, @Depth_DIFF, @file_DIFF
					 END
				  close CUR
				  DEALLOCATE CUR
		END
		else -- Without a Date Sub Folder
		BEGIN
			   PRINT ''
			 print 'The BACKUP_FOLDER_DIFF is without a DATE'
			 Print ''
			 
			   IF @BACKUP_FOLDER_DIFF IS NOT NULL 
				   BEGIN
					IF @BACKUP_FOLDER_DIFF != N'' 
					BEGIN
					 IF @SHOW_MESSAGE_FLAG = 1 
					 BEGIN
						PRINT (N' (i) Populating temporary table for DIFF files...')
						PRINT (N'   DIFF folder: "'+ISNULL(@BACKUP_FOLDER_DIFF, N'')+N'"')
						PRINT (N'')
					 END   
					 INSERT INTO #tblDir EXEC master..xp_dirtree @BACKUP_FOLDER_DIFF, 1, 1
					END
				  END
		 END
			  
select * from #tblDir
END

-----------------------------------------------------------------------------------------------------------------------------------------------
--CONTINUES ->----------------------------------------------------------------------------------------------------------------------------------

print 'SHOW_MESSAGE_FLAG:= ' + cast(@SHOW_MESSAGE_FLAG as NVARCHAR(20))

If @SHOW_MESSAGE_FLAG = 1 SELECT * FROM #tblDir 

select * from #tblDir


print '@SOURCE_SERVERNAME := ' + @SOURCE_SERVERNAME
print '@BACKUP_FOLDER_DATA := ' + @BACKUP_FOLDER_DATA
print '@SOURCE_DATABASE := ' + @SOURCE_DATABASE
--declare @r as NVARCHAR(1200) = LEFT('DBASRV-DEV$SQL2008_Anuunaki_20140312_1612_58_120_DATA.sqmc', LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1)
--declare @f as NVARCHAR(1200) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
--IF ( @r= @f)
--BEGIN
--print 'Yesssss'
--END

--print LEFT('DBASRV-DEV$SQL2008_Anuunaki_20140312_1612_58_120_DATA.sqmc',LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1)
--print REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'



IF (@Is_Contains_Date = 1 ) 
BEGIN
SELECT TOP 1
@LAST_DATA_BACKUP_FILE = @BACKUP_FOLDER_DATA+[subdirectory],
@LAST_DATA_BACKUP_DATE = CAST(SUBSTRING([subdirectory], LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS INT),
@LAST_DATA_BACKUP_TIME = CAST(SUBSTRING([subdirectory], LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS INT),
@LAST_DATA_BACKUP_COMPRESSED_FLAG = CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT)
FROM 
#tblDir 
WHERE 
[file] = 1
AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
AND CHARINDEX('_DATA.',[subdirectory],1)>0
AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
ORDER BY
SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) DESC, 
SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) DESC

END

ELSE

BEGIN
SELECT TOP 1
@LAST_DATA_BACKUP_FILE = @BACKUP_FOLDER_DATA+[subdirectory],
@LAST_DATA_BACKUP_DATE = CAST(SUBSTRING([subdirectory], LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS INT),
@LAST_DATA_BACKUP_TIME = CAST(SUBSTRING([subdirectory], LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS INT),
@LAST_DATA_BACKUP_COMPRESSED_FLAG = CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT)
FROM 
#tblDir 
WHERE 
[file] = 1
--AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
AND CHARINDEX('_DATA.',[subdirectory],1)>0
AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
ORDER BY
SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) DESC, 
SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) DESC

END

--First we'll get the last full data backup file AND the date/time it occurred...

print ''
print  '@LAST_DATA_BACKUP_FILE := ' + cast(@LAST_DATA_BACKUP_FILE as NVARCHAR(1200))
print   '@LAST_DATA_BACKUP_DATE := ' +  cast(@LAST_DATA_BACKUP_DATE as NVARCHAR(1200))
print  '@LAST_DATA_BACKUP_TIME := ' +  cast(@LAST_DATA_BACKUP_TIME as NVARCHAR(1200))
print '@LAST_DATA_BACKUP_COMPRESSED_FLAG := ' +  cast(@LAST_DATA_BACKUP_COMPRESSED_FLAG as NVARCHAR(1200))
print ''
--------------------------------------------------

IF @SHOW_MESSAGE_FLAG = 1 
BEGIN
PRINT (N' (i) Last full data backup:')
PRINT (N'   File: "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"')
PRINT (N'   Date: "'+CAST(ISNULL(@LAST_DATA_BACKUP_DATE, 0) AS NVARCHAR(80))+N' '+REPLICATE(N'0', 4 - LEN(CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80))))+CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80))+N'"')
PRINT (N'   Compressed: "'+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 0) AS NVARCHAR(80))+N'"')
PRINT (N'')
END


SELECT * FROM master..sysdatabases WHERE [name] = @DESTINATION_DB_NAME

IF NOT EXISTS (SELECT * FROM master..sysdatabases WHERE [name] = @DESTINATION_DB_NAME)
BEGIN
--The destination database doesnt exist and needs to be created, we need to get the last DATA backup file
--and create the database from it...
IF (@LAST_DATA_BACKUP_FILE IS NULL) OR (@LAST_DATA_BACKUP_FILE = N'')
BEGIN
  PRINT(N' - No DATA backup was found, unable to create the destination database, DRP process is aborted')
  GOTO FinishDrp
END

--Before creating the database, we need to check and verIFy the compatibility level according to the source
--database, IF this is an upgrade - a "standby" restore will not succeed, so we'll need just to keep the database
--open.
--Because the database doesnt exist (we ARE going to create it) we'll check the compatibility level of
--the "model" database
--print '@SOURCE_DATABASE_COMPATIBILITY_LEVEL := ' + cast(@SOURCE_DATABASE_COMPATIBILITY_LEVEL as NVARCHAR(1200))
declare @t bit
SELECT TOP 1 @t = [cmptlevel] FROM master..sysdatabases WHERE [name] = N'model'
print '(SELECT TOP 1 [cmptlevel] FROM master..sysdatabases WHERE [name] := ' +  cast(@t as NVARCHAR(1))


------------------Yehoda Lasri Change-----------------------------------------------------------------------------------------
print '@RESTORE_WITH_STANDBY_FLAG before yehoda: ' +  cast(@RESTORE_WITH_STANDBY_FLAG as NVARCHAR(1))
print ' YEHODA CHANGE ' 
IF @SOURCE_VERSION IS NOT NULL AND @DESTINATION_SQL_VERSION IS NOT NULL AND @SOURCE_VERSION<>@DESTINATION_SQL_VERSION
	  BEGIN 
			SET @RESTORE_WITH_STANDBY_FLAG=0   
	   END 
	   ELSE
			BEGIN  
			 IF @SOURCE_DATABASE_COMPATIBILITY_LEVEL < (SELECT TOP 1 [cmptlevel] FROM master..sysdatabases WHERE [name] = N'model')
				   SET @RESTORE_WITH_STANDBY_FLAG = 0
					ELSE
					SET @RESTORE_WITH_STANDBY_FLAG = 1
			END

--Instead of this code----------------------------------------------------------------------------------------------------------
--IF @SOURCE_DATABASE_COMPATIBILITY_LEVEL < (SELECT TOP 1 [cmptlevel] FROM master..sysdatabases WHERE [name] = N'model')
--  SET @RESTORE_WITH_STANDBY_FLAG = 0
--ELSE
--  SET @RESTORE_WITH_STANDBY_FLAG = 1
-------------------------------------------------------------------------------------------------------------------------------


print '@RESTORE_WITH_STANDBY_FLAG := ' + cast(@RESTORE_WITH_STANDBY_FLAG as NVARCHAR(1))

--BUT, IF this is a DATA based DRP then we don't need to "wait" for more logs/dIFfs and the database can be
--upgraded IF needed...

print '@DRP_TYPE := ' + @DRP_TYPE

IF (@DRP_TYPE = N'DATA') AND (@SOURCE_VERSION IS NOT NULL AND @DESTINATION_SQL_VERSION IS NOT NULL AND @SOURCE_VERSION = @DESTINATION_SQL_VERSION) SET @RESTORE_WITH_STANDBY_FLAG = 1

PRINT(N' - VerIFying access to DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
IF @FILE_ACCESS_CHECK = 0
BEGIN
  SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"'
  RAISERROR(@ERR_MSG, 16, 1)
  GOTO FinishDrp
END

--create the database from the last backup file...
--   consider compatibility level
--   keep open / read_only...? (check which DRP type we need...)
PRINT(N' - Creating database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')

IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1
SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
	+N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))

IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))
IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)

SET @CMD_START=GETDATE()
EXEC master..sp_executesql @CMD
SET @ERROR=@@ERROR
IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END





--IF the database was just created, we do need to perform a dIFferential restore IF necessary,
--so we'll put the last data backup date/time as the last restore date/time, and thus - IF there are
--any newer files for the sequence they will be updated...

print '@@LAST_DATA_BACKUP_DATE : = ' + cast(@LAST_DATA_BACKUP_DATE as NVARCHAR(1200))


SET @LAST_RESTORE_DATE = @LAST_DATA_BACKUP_DATE
SET @LAST_RESTORE_TIME = @LAST_DATA_BACKUP_TIME



print '@LAST_RESTORE_DATE : = ' + cast(@LAST_RESTORE_DATE as NVARCHAR(1200))
print '@LAST_RESTORE_TIME : = ' + cast(@LAST_RESTORE_TIME as NVARCHAR(1200))

--When creating the database from the last DATA backup file, we need to consider IF this is a DATA based DRP or
--not, IF this is not a DATA based DRP we need also to take the compatibility level under consideration, because
--a database cannot stay in a "standby" mode IF we need to upgrade it...


--After creating the database from the last DATA backup file, we need to check, IF this is a DATA based DRP
--then we can finish the process here, BUT IF this is a log/dIFf based DRP than the process needs to continue
--because all other LOGS or the DIFF needs to be added to the database...
IF @DRP_TYPE = N'DATA' GOTO FinishDrp
END

ELSE --EXIST EXIST EXIST--------------EXIST EXIST EXIST--------------------EXIST EXIST EXIST-----------------------EXIST EXIST EXIST-------------------EXIST EXIST EXIST
BEGIN

--IF the database already exists, we'll get its last restore date/time...
--Getting the last restore date and time of the DRP database... (now that we know that the database exists...)

print 'LAST_RESTORE_TIME : = ' + cast(@LAST_RESTORE_TIME as NVARCHAR(1200))
print '@DESTINATION_DB_NAME : = ' + cast(@DESTINATION_DB_NAME as NVARCHAR(1200))
print 'LAST_RESTORE_TIME : = ' + cast(@LAST_RESTORE_TIME as NVARCHAR(1200))

SELECT TOP 1 @LAST_RESTORE_DATE = CAST(CONVERT(NVARCHAR(80), [restore_date], 112) AS INT), @LAST_RESTORE_TIME = CAST(LEFT(REPLACE(CONVERT(NVARCHAR(80), [restore_date], 108), N':', N''), 4) AS INT) 
 FROM msdb..restorehistory WHERE destination_database_name = @DESTINATION_DB_NAME ORDER BY restore_date DESC

print ''
print '@LAST_RESTORE_DATE := ' + cast(@LAST_RESTORE_DATE as NVARCHAR(1200))
print ''
print ''
print '@SOURCE_DATABASE_COMPATIBILITY_LEVEL := ' + cast(@SOURCE_DATABASE_COMPATIBILITY_LEVEL as NVARCHAR(1200))
print ''

declare @tt NVARCHAR(1200) 

SELECT TOP 1 @tt = [cmptlevel] FROM master..sysdatabases WHERE [name] = N'model'

print '@tt' + @tt

--Here we know that the database exists, we'll check the compatibility level of the server to know IF to
--set it to "standby" or just "recovering" in future restore operations...

----------------------------Yehoda Lasri Change-----------------------------------------------------------------------------------
print '@RESTORE_WITH_STANDBY_FLAG before yehoda: ' +  cast(@RESTORE_WITH_STANDBY_FLAG as NVARCHAR(1))
print ' YEHODA CHANGE ' 
IF @SOURCE_VERSION IS NOT NULL AND @DESTINATION_SQL_VERSION IS NOT NULL AND @SOURCE_VERSION<>@DESTINATION_SQL_VERSION
	  BEGIN 
			SET @RESTORE_WITH_STANDBY_FLAG=0   
	   END 
	   ELSE
			BEGIN  
			 IF @SOURCE_DATABASE_COMPATIBILITY_LEVEL < (SELECT TOP 1 [cmptlevel] FROM master..sysdatabases WHERE [name] = N'model')
					SET @RESTORE_WITH_STANDBY_FLAG = 0
					ELSE
					SET @RESTORE_WITH_STANDBY_FLAG = 1
			END 
--------------Instead of this code ------------------------------------------------------------------------------------------------
--IF @SOURCE_DATABASE_COMPATIBILITY_LEVEL < (SELECT TOP 1 [cmptlevel] FROM master..sysdatabases WHERE [name] = N'model')
--  SET @RESTORE_WITH_STANDBY_FLAG = 0
--ELSE
--  SET @RESTORE_WITH_STANDBY_FLAG = 1
-----------------------------------------------------------------------------------------------------------------------------------


print '@RESTORE_WITH_STANDBY_FLAG := ' + cast(@RESTORE_WITH_STANDBY_FLAG as NVARCHAR(1200))

--BUT, IF this is a DATA based DRP then we don't need to "wait" for more logs/dIFfs and the database can be
--upgraded IF needed...
IF @DRP_TYPE = N'DATA' SET @RESTORE_WITH_STANDBY_FLAG = 1
END

--In case no restore date is available - we'll set it to 0 (zero) so the queries will run properly
IF @LAST_RESTORE_DATE IS NULL SET @LAST_RESTORE_DATE = 0
IF @LAST_RESTORE_TIME IS NULL SET @LAST_RESTORE_TIME = 0

print ''
print '@LAST_RESTORE_DATE := ' + cast (@LAST_RESTORE_DATE as NVARCHAR(1200))
print '@LAST_RESTORE_TIME := ' +  cast (@LAST_RESTORE_TIME as NVARCHAR(1200))
print ''

--Now, according to the DRP type and database status we change the selection of the backup files involved in
--the process:
--IF this is a DATA based DRP, we need to get only the last DATA backup (this will be what the table contains)
--IF this is a DIFF based DRP, we need to get the last last DIFF backup of the database 
--    later on in the actual restore process we'll need to consider IF the database is currently in standby mode or not...
--    and IF we need to also restore the full data backup (which we got earlier)
--IF this is a LOG based DRP, we need to get all LOG backups made from the LAST DATA BACKUP (and not necessarily from the last restore 
--    this is because that IF the database is not currently in standby - we'll need the entire sequence of log backups
--    in order to get it to the current state.

---------------------------------------.
print 'DRP_TYPE : = ' + @DRP_TYPE

IF @DRP_TYPE = N'DATA'
BEGIN
--This is a full data DRP, we'll check IF the last data backup is newer than the last restore of the destination
--database, IF so - we'll need to perform a restore.
IF (@LAST_DATA_BACKUP_FILE IS NULL) OR (@LAST_DATA_BACKUP_FILE = N'')
BEGIN
  PRINT(N' - No DATA backup was found, DRP process is aborted')
  GOTO FinishDrp
END

PRINT(N' - Database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" last restore date: '+CAST(ISNULL(@LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+N' '+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80)))
IF CAST(CAST(ISNULL(@LAST_DATA_BACKUP_DATE, 0) AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_DATA_BACKUP_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_DATA_BACKUP_TIME, 0) AS NVARCHAR(80)) AS BIGINT) >= CAST(CAST(ISNULL(@LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80)) AS BIGINT)
BEGIN
  --The last DATA backup is newer - we need to restore it...
  --  NOTE: when restoring we need to check IF the user wants to keep the database in
  --  read_only mode or not...

  --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
  PRINT(N' - VerIFying access to DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
  EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
  IF @FILE_ACCESS_CHECK = 0
  BEGIN
   SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"'
   RAISERROR(@ERR_MSG, 16, 1)
   GOTO FinishDrp
  END

  --Now the actual restore...
  PRINT(N' - Restoring database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  
  IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1
  SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
		+N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))
IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
  IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
  IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
  SET @CMD_START=GETDATE()
  EXEC master..sp_executesql @CMD
  SET @ERROR=@@ERROR
  IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END
	
  
 END
  ELSE
BEGIN
  --The last DATA backup is not newer than the last restore, we dont need to do anything...
  PRINT(N' - No newer DATA backup was found, DRP process is completed')
  GOTO FinishDrp
END
END
---------------------------------------

IF @DRP_TYPE = N'DIFF'
BEGIN
print''
print 'Inside The DIFF CASE'
print''

--This is a dIFferential DRP, we need to get the last dIFferential backup,
--   BUT - we also need to check IF the last data backup of that database is newer than the last restore
--   because IF it is - we'll need to also restore the DATA itself...
--   after we check the data, we'll do the same check for the DIFF backup, IF it is not newer than the
--   last restore, we don't need to restore it...

--First - we'll check the last data backup and see IF we need to perform a full restore
PRINT(N' - Database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" last restore date: '+CAST(ISNULL(@LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+N' '+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80)))

--todo
  --16/02/2011 
 --Yehuda.lasri Add Flag To SKIP ON FULL BACKUP WHEN Destination Database EXISTS And On stendBy Mode
IF DATABASEPROPERTYEX(@SOURCE_DATABASE + @DESTINATION_DATABASE_NAME_SUFFIX, 'status')='RESTORING' 
					  OR DATABASEPROPERTYEX( @SOURCE_DATABASE +@DESTINATION_DATABASE_NAME_SUFFIX , 'IsInStandBy' )=1 
 SET @DO_NOT_RUN_BACKUP=1

IF ( @DO_NOT_RUN_BACKUP=0 OR @FORCE_DATABASE_RESTORE=1) AND CAST(CAST(ISNULL(@LAST_DATA_BACKUP_DATE, 0) AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_DATA_BACKUP_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_DATA_BACKUP_TIME, 0) AS NVARCHAR(80)) AS BIGINT) >= CAST(CAST(ISNULL(@LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80)) AS BIGINT)
BEGIN
  --The last DATA backup is newer - we need to restore it, and only then apply the new DIFF to it (IF any)
  --  NOTE: when restoring the DATA, we need to keep the database in "standby" or "recovering" mode in
  --  order to be able to apply the rest of the files (logs/dIFfs)...

  --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
  PRINT(N' - VerIFying access to DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
  EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
  IF @FILE_ACCESS_CHECK = 0
  BEGIN
   SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"'
   RAISERROR(@ERR_MSG, 16, 1)
   GOTO FinishDrp
  END

print @LAST_DATA_BACKUP_FILE
  --Now - the restore command...
  PRINT(N' - Restoring database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1
  SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
			+N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))
  IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
  IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
  IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
  SET @CMD_START=GETDATE()
  EXEC master..sp_executesql @CMD
  SET @ERROR=@@ERROR
  IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END
END
  ELSE
BEGIN
  --The last DATA backup is not newer than the last restore, and currently doesnt need to be restored.
  PRINT(N' - No newer DATA backup was found, continue with dIFferential DRP process')
END


----------Added By Israel Eitan Pro on 21/03/2014-------------------Take care of DIFf DRP with Date Folder---------------------------------------------------------
Declare @LastDIFfFile NVARCHAR(1200) 
set  @LastDIFfFile = N''
Declare @LastDIFfFolder NVARCHAR(1200) 
set @LastDIFfFolder = N''

select * from #tblDir order by [subdirectory] desc

IF (@Is_Contains_Date = 0 ) 
	  BEGIN
	  -- With Date as a SubFolder
			print ''
			print 'With Date as a SubFolder'
			print''
			
				  SELECT top 1 @LastDIFfFile =  [subdirectory] --Latest DIFf File
				  from #tblDir 
				  where [subdirectory] not like '%standby'
				  order by  [subdirectory] desc 
				  
				  SELECT top 1 @LastDIFfFolder =  [subdirectory] --Latest DIFf Folder
				  from #tblDIFFMain 
				  order by  [subdirectory] desc 
				  
				  print '@LastDIFfFile : = ' + @LastDIFfFile
				print '@LastDIFfFolder : = ' + @LastDIFfFolder
				  --Compose "new" correct folder for the latest dIFf file
				  SET @LastDIFfFolder = substring(@BACKUP_FOLDER_DIFF,0,CHARINDEX ('DIFF',@BACKUP_FOLDER_DIFF,0)+4) + '\' + @LastDIFfFolder + '\'
	  END
else
	  BEGIN
			-- Without Date as a SubFolder
			print ''
			print 'Without Date as a SubFolder'
			print''
			
				  SELECT top 1 @LastDIFfFile =  [subdirectory] --Latest DIFf File
				  from #tblDir 
				  order by  [subdirectory] desc
				  
				  SET @LastDIFfFolder = @BACKUP_FOLDER_DIFF
	  END



SELECT top 1
  @LAST_DIFF_BACKUP_FILE = @LastDIFfFolder+@LastDIFfFile,
  @LAST_DIFF_BACKUP_DATE = CAST(SUBSTRING(@LastDIFfFile, LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS INT),
  @LAST_DIFF_BACKUP_TIME = CAST(SUBSTRING(@LastDIFfFile, LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS INT),
  @LAST_DIFF_BACKUP_COMPRESSED_FLAG = CAST((CASE CHARINDEX(N'.', REVERSE(@LastDIFfFile)) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE(@LastDIFfFile), 1, CHARINDEX(N'.', REVERSE(@LastDIFfFile)) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT)
FROM 
  #tblDir 
 WHERE 
  [file] = 1
  AND LEFT(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
  AND ISNUMERIC(SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
  AND ISNUMERIC(SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
  AND RIGHT(@LastDIFfFile, LEN(N'.standby')) != N'.standby'
  AND CHARINDEX('_DIFF.',@LastDIFfFile,1)>0
  AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM(@LastDIFfFile))) = 0
ORDER BY
  SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) desc, 
  SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) desc


--After we checked and restored the full DATA backup IF needed, we'll continue with the
--dIFferential drp process, we need to get the last DIFF backup file
SELECT TOP 1
  @LAST_DIFF_BACKUP_FILE = @LastDIFfFolder+@LastDIFfFile,
  @LAST_DIFF_BACKUP_DATE = CAST(SUBSTRING(@LastDIFfFile, LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS INT),
  @LAST_DIFF_BACKUP_TIME = CAST(SUBSTRING(@LastDIFfFile, LEN(@SOURCE_SERVERNAME) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS INT),
  @LAST_DIFF_BACKUP_COMPRESSED_FLAG = CAST((CASE CHARINDEX(N'.', REVERSE(@LastDIFfFile)) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE(@LastDIFfFile), 1, CHARINDEX(N'.', REVERSE(@LastDIFfFile)) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT)
FROM 
  #tblDir 
 WHERE 
  [file] = 1
  AND LEFT(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
  AND ISNUMERIC(SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
  AND ISNUMERIC(SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
  AND RIGHT(@LastDIFfFile, LEN(N'.standby')) != N'.standby'
  AND CHARINDEX('_DIFF.',@LastDIFfFile,1)>0
  AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM(@LastDIFfFile))) = 0
ORDER BY
  SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) desc, 
  SUBSTRING(@LastDIFfFile, LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) desc

print ' @LAST_DIFF_BACKUP_FILE : = ' + @LAST_DIFF_BACKUP_FILE

IF CAST(CAST(@LAST_DIFF_BACKUP_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LAST_DIFF_BACKUP_TIME AS NVARCHAR(80))))+CAST(@LAST_DIFF_BACKUP_TIME AS NVARCHAR(80)) AS BIGINT) >= CAST(CAST(@LAST_RESTORE_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LAST_RESTORE_TIME AS NVARCHAR(80))))+CAST(@LAST_RESTORE_TIME AS NVARCHAR(80)) AS BIGINT)
BEGIN
  --The last DIFF backup is newer - we need to restore it...
  --  BUT, before we restore the DIFF, we do need to check IF the database is ready for the file to be restored,
  --  which means that we need to verIFy that the database is currently in "standby" or "recovering" mode, IF it
  --  is not - we'll still need to restore the full DATA backup.
  IF (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'IsInStandby') AS INT) = 0) AND (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) != N'RECOVERING') AND (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) != N'RESTORING')
  BEGIN
   --The database is NOT in "standby" nor in "recovering" mode, which means that we must re-restore the full DATA
   --backup in order to be able to apply dIFferential backups...

   --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
   PRINT(N' - VerIFying access to DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
   SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
   EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
   IF @FILE_ACCESS_CHECK = 0
   BEGIN
	SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"'
	RAISERROR(@ERR_MSG, 16, 1)
	GOTO FinishDrp
   END

   --Now, the actual restore command...
   print @LAST_DATA_BACKUP_FILE
   PRINT(N' - Restoring database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
   IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1
   SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
			+N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))
   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   SET @CMD_START=GETDATE()
   EXEC master..sp_executesql @CMD
   SET @ERROR=@@ERROR
   IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END
   END

  IF (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'IsInStandBy') AS INT) = 1) OR (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) = N'RECOVERING') OR (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) = N'RESTORING')
  BEGIN
   --The database is either in "standby" or "recovering" mode, which means it is ready for
   --dIFferential restore...


   
   --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
   PRINT(N' - VerIFying access to DIFF backup file "'+ISNULL(@LAST_DIFF_BACKUP_FILE, N'')+N'"...')
   SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DIFF_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
   EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
   IF @FILE_ACCESS_CHECK = 0
   BEGIN
	SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DIFF_BACKUP_FILE, N'')+N'"'
	RAISERROR(@ERR_MSG, 16, 1)
	GOTO FinishDrp
   END

   --Now, the actual restore command...
   PRINT(N' - Restoring database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DIFF backup file "'+ISNULL(@LAST_DIFF_BACKUP_FILE, N'')+N'"...')
   IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1
   SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''I'', @FILENAME = N'''+ISNULL(@LAST_DIFF_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DIFF_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
				+N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))
   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   SET @CMD_START=GETDATE()
   EXEC master..sp_executesql @CMD
   SET @ERROR=@@ERROR
   IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END

   --After restoring the dIFferential, finishing the DRP process.
   PRINT(N' - DIFferential DRP process is completed.')
   GOTO FinishDrp
  END
   ELSE
  BEGIN
   --The database is NOT in the correct status to apply a dIFferential backup, an error will be issued.
   RAISERROR(N'Unable to apply dIFferential backup to a database that is not in "standby" nor "recovering" mode.', 16, 1)
   GOTO FinishDrp
  END
END
  ELSE
BEGIN
  --The last DIFF backup is not newer than the last restore, we dont need to do anything...
  PRINT(N' - No newer DIFF backup was found, DRP process is completed')
  GOTO FinishDrp
END 
END





---------------------------------------
IF @DRP_TYPE = N'LOG'
BEGIN
print ' '
print 'Inside need to restore the entire sequence'
print ''
--This is a log based DRP, which is the "most" complicated,
--   first - we need to check IF the DATA needs to be restored,
--   IF the DATA backup is newer and needs to be restored, we'll also need to apply ALL the log backup files
--   since that backup
--   BUT - IF the DATA backup is not newer, we'll need to apply only NEWER logs, but still
--   we do need to check IF the database is in standby mode or not, becase IF it is not - we'll still
--   need to restore the entire sequence (that is why when selecting the newer logs - we'll select all logs newer than the last DATA backup)

--First - we'll check the last data backup and see IF we need to perform a full restore

PRINT(N' - Database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" last restore date: '+CAST(ISNULL(@LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+N' '+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80)))

--todo
--16/02/2011 
 --Yehuda.lasri Add Flag To SKIP ON FULL BACKUP WHEN Destination Database EXISTS And On StENDBy Mode
IF DATABASEPROPERTYEX(@SOURCE_DATABASE + @DESTINATION_DATABASE_NAME_SUFFIX, 'status')='RESTORING' 
					  OR DATABASEPROPERTYEX( @SOURCE_DATABASE +@DESTINATION_DATABASE_NAME_SUFFIX , 'IsInStandBy' )=1 
 SET @DO_NOT_RUN_BACKUP=1

print ' '
print '@DO_NOT_RUN_BACKUP := ' + cast(@DO_NOT_RUN_BACKUP as NVARCHAR(1200))
print '' 
 print ' '
print '@@FORCE_DATABASE_RESTORE := ' + cast(@FORCE_DATABASE_RESTORE as NVARCHAR(1200))
print '' 
 print ' '
print '@@@LAST_DATA_BACKUP_DATE := ' + cast(@LAST_DATA_BACKUP_DATE as NVARCHAR(1200))
print '' 
 print ' '
print '@@@LAST_DATA_BACKUP_TIME := ' + cast(@LAST_DATA_BACKUP_TIME as NVARCHAR(1200))
print '' 
 

 IF (@DO_NOT_RUN_BACKUP=0 OR @FORCE_DATABASE_RESTORE=1) AND CAST(CAST(ISNULL(@LAST_DATA_BACKUP_DATE, 0) AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_DATA_BACKUP_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_DATA_BACKUP_TIME, 0) AS NVARCHAR(80)) AS BIGINT) >= CAST(CAST(ISNULL(@LAST_RESTORE_DATE, 0) AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80))))+CAST(ISNULL(@LAST_RESTORE_TIME, 0) AS NVARCHAR(80)) AS BIGINT)
BEGIN
  --The last DATA backup is newer - we need to restore it, and then apply ALL newer log files...
  --  NOTE: when restoring the DATA, we need to keep the database in "standby" or "recovering" mode in
  --  order to be able to apply the rest of the files (logs/dIFfs)...



  --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
  PRINT(N' - VerIFying access to DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
  EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
  IF @FILE_ACCESS_CHECK = 0
  BEGIN
   SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"'
   RAISERROR(@ERR_MSG, 16, 1)
   GOTO FinishDrp
  END

  
  --Now, the actual restore command...
  PRINT(N' - Restore database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DATA backup "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1

  SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+
			 N''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+
			 N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+
			 N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+
			 N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
			 +N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))

  IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
  IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
  IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
  SET @CMD_START=GETDATE()
  print @CMD
  EXEC master..sp_executesql @CMD
  SET @ERROR=@@ERROR
  IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END
END
  ELSE
BEGIN
  --The last DATA backup is not newer than the last restore, and currently doesnt need to be restored.
  PRINT(N' - No newer DATA backup was found, continue with log DRP process')
END

--After we check and verIFied IF a DATA restore was needed, we need to check the recovery of the
--database, IF the database is not in "standby" or "recovering" mode, we need to restore the full DATA
--anyway. (here this is done even before we query the log backups list, because IF the database needs to
--be fully restored, all logs from the last backup needs to be applied, otherwise, only new logs - from
--the last restore needs to be applied).

declare @i int
declare @c NVARCHAR(80)
declare @d NVARCHAR(80)
SET @i = CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'IsInStandby') AS INT)
SET @c = CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80))
SET @d = CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) 

print ' '
print 'IF NUMBER 1 -> CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N''IsInStandby'') AS INT) : = ' + cast(@i as NVARCHAR(20))
print 'IF NUMBER 2 -> CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N''Status'') AS NVARCHAR(80)) : = ' + @c
print 'IF NUMBER 3 -> CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N''Status'') AS NVARCHAR(80)) : = ' + @d
print ''

IF (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'IsInStandby') AS INT) = 0) AND (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) != N'RECOVERING') AND (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) != N'RESTORING')
BEGIN
  
  print ' '
  print 'INSIDE IF NUMBER 1 '
  print ' '

  --The database is NOT in "standby" nor in "recovering" mode, which means that we must re-restore the full DATA
  --backup in order to be able to apply the log backups...

  --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
  print ' '
  PRINT(N' - VerIFying access to DATA backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  print ' '
  SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
  EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
  IF @FILE_ACCESS_CHECK = 0
  BEGIN
   SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"'
   RAISERROR(@ERR_MSG, 16, 1)
   GOTO FinishDrp
   END
   ELSE
		 BEGIN
		 print ' '
		 print 'Verification Secceded : File Exist!'
		 print ' '
		 END
  

  --Now, the actual restore command...
  Print ' '
  
  PRINT(N' - Restore database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from DATA backup "'+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N'"...')
  print ' '
  IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1

  SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''D'', @FILENAME = N'''+ISNULL(@LAST_DATA_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LAST_DATA_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = 1'
			 +N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))

  IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
  IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
  IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
  SET @CMD_START=GETDATE()
  EXEC master..sp_executesql @CMD
  SET @ERROR=@@ERROR
  IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
		   16, -- Severity,
		   1, -- State,
		   @CMD, -- First argument.
			   @CMD_DURATION,
		   @ERROR) WITH LOG  
	
	END

print ''
  print 'Here WE Update the LAST DATABSE RESTORE date after we seccesfuly restored the database' 
  print ''
  
  --After we restored the database, we DO need to update the "last database restore" date and time...
  PRINT(N' - Updating last restore information from database...')
  SELECT TOP 1 @LAST_RESTORE_DATE = CAST(CONVERT(NVARCHAR(80), [restore_date], 112) AS INT), @LAST_RESTORE_TIME = CAST(LEFT(REPLACE(CONVERT(NVARCHAR(80), [restore_date], 108), N':', N''), 4) AS INT) FROM msdb..restorehistory WHERE destination_database_name = @DESTINATION_DB_NAME ORDER BY [restore_date] DESC
	print ''
  print '@LAST_RESTORE_DATE := ' + cast(@LAST_RESTORE_DATE as NVARCHAR(800))
  print ''


--SELECT 
--    substring(@BACKUP_FOLDER_LOG,0,charindex('LOG',@BACKUP_FOLDER_LOG,0)+4) +
--    SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) + '\' +[subdirectory], 
--    SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS [backup_date],
--    SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS [backup_time],
--    CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT) AS [compressed_flag]
--   FROM 
--    #tblDir 
--   WHERE 
--    [file] = 1
--    AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
--    AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
--    AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
--    AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
--    AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LAST_DATA_BACKUP_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80))))+CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80)) AS BIGINT)
--    AND CHARINDEX('_LOG.',[subdirectory],1)>0
--    AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
--   ORDER BY
--    [backup_date] ASC, [backup_time] ASC

----------ADDED By Israel Eitan Pro on 13/03/2014--------------------------------Log Files with a Date Sub folder-----------------------------------------
IF (  @Is_Contains_Date = 0 ) 

	  BEGIN
	  print 'With Date Folder'
		--With Date Folder
		--After restoring the full database, we need to query the list of backups for ALL log backups that are newer
		--than the last DATA backup and apply them to the database.
	   DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
		FOR
		 SELECT 
			substring(@BACKUP_FOLDER_LOG,0,charindex('LOG',@BACKUP_FOLDER_LOG,0)+4) +
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) + '\' +[subdirectory], 
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS [backup_date],
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS [backup_time],
			CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT) AS [compressed_flag]
		 FROM 
			#tblDir 
		 WHERE 
			[file] = 1
			AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
			AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
			AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LAST_DATA_BACKUP_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80))))+CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80)) AS BIGINT)
			AND CHARINDEX('_LOG.',[subdirectory],1)>0
			AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
		 ORDER BY
			[backup_date] ASC, [backup_time] ASC
		OPEN CUR
		FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
	  END
else

	  BEGIN
	  print 'Without Date Folder'
		--Without Date Folder
		--After restoring the full database, we need to query the list of backups for ALL log backups that are newer
		--than the last DATA backup and apply them to the database.
		DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
		FOR
		 SELECT 
		 @BACKUP_FOLDER_LOG+[subdirectory],
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS [backup_date],
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS [backup_time],
			CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT) AS [compressed_flag]
		 FROM 
			#tblDir 
		 WHERE 
			[file] = 1
			AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
			AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
			AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LAST_DATA_BACKUP_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80))))+CAST(@LAST_DATA_BACKUP_TIME AS NVARCHAR(80)) AS BIGINT)
			AND CHARINDEX('_LOG.',[subdirectory],1)>0
			AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
		 ORDER BY
			[backup_date] ASC, [backup_time] ASC
		OPEN CUR
		FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
	  END

  
  WHILE @@FETCH_STATUS = 0
  BEGIN
   --Restore the log file to the database...
	  BEGIN TRY

   --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
   PRINT(N' - VerIFying access to LOG backup file "'+ISNULL(@LOG_BACKUP_FILE, N'')+N'"...')
   SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LOG_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
   EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
   IF @FILE_ACCESS_CHECK = 0
   BEGIN
	SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LOG_BACKUP_FILE, N'')+N'"'
	RAISERROR(@ERR_MSG, 16, 1)
	GOTO FinishDrp
   END
   
   
   
 print @LOG_BACKUP_FILE
   --Now, the actual restore command...
   PRINT(N' - Restore database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from LOG backup "'+ISNULL(@LOG_BACKUP_FILE, N'')+N'"...')
   IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1

   SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''L'', @FILENAME = N'''+ISNULL(@LOG_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LOG_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))+
			N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))

   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   SET @CMD_START=GETDATE()
   print @CMD
   EXEC master..sp_executesql @CMD
   SET @ERROR=@@ERROR
   IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	--RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
	--       16, -- Severity,
	--       1, -- State,
	--       @CMD, -- First argument.
	--           @CMD_DURATION,
   --       @ERROR) WITH LOG  
	
	END

  FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
  
  END try
		BEGIN CATCH
		FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
		  print 'Error : ' + ERROR_MESSAGE()
	  --   CONTINUE
		END CaTCH
  END
  
  CLOSE CUR
  DEALLOCATE CUR

  --When finishing the sequence restore, completing the process.
  PRINT(N' - Log DRP process is completed.')
  GOTO FinishDrp
  
END

--IF we got here, it means that the database is either in "standby" or "recovering", we'll verIFy it...
IF (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'IsInStandBy') AS INT) = 1) OR (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) = N'RECOVERING') OR (CAST(DATABASEPROPERTYEX(@DESTINATION_DB_NAME, N'Status') AS NVARCHAR(80)) = N'RESTORING')
BEGIN
  --This means the database is in the correct mode (either "standby" or "recovering"), we need to get
  --the list of all LOG backups since the last database restore.

  --BUT, IF the last restore was a FULL data restore, it means we need to restore all logs since that
  --backup...
---*****
  --Preparing variables for the log restore list...
  IF EXISTS (SELECT TOP 1 [restore_type] FROM msdb..restorehistory WHERE destination_database_name = @DESTINATION_DB_NAME ORDER BY restore_date DESC)
  BEGIN
  
	 SELECT TOP 1 
	@LOG_BACKUP_LIST_DATE = CAST(CONVERT(NVARCHAR(80), bkset.backup_finish_date, 112) AS INT),
	@LOG_BACKUP_LIST_TIME = CAST(LEFT(REPLACE(CONVERT(NVARCHAR(80), bkset.backup_finish_date, 108), N':', N''), 4) AS INT)
   FROM 
	msdb..restorehistory res
  INNER JOIN
	msdb..backupset bkset
	  ON res.backup_set_id = bkset.backup_set_id
   WHERE 
	destination_database_name = @DESTINATION_DB_NAME 
   ORDER BY
	res.restore_date DESC
	
   ---The last restore was a log restore, which means we need to get all log files after the last restore...
   -- SELECT TOP 1 
   -- @LOG_BACKUP_LIST_DATE = CAST(CONVERT(NVARCHAR(80), restore_date, 112) AS INT),
   -- @LOG_BACKUP_LIST_TIME = CAST(LEFT(REPLACE(CONVERT(NVARCHAR(80), restore_date, 108), N':', N''), 4) AS INT)
   --FROM 
   --msdb..restorehistory res
   --WHERE 
   --destination_database_name = @DESTINATION_DB_NAME 
   --ORDER BY
   -- res.restore_date DESC

  END
   ELSE
  BEGIN
   --The last restore was a full backup file or a dIFferential backup file, which means the sequence should include
   --all log backup files after it...
   
	 
   SET @LOG_BACKUP_LIST_DATE = @LAST_DATA_BACKUP_DATE
   SET @LOG_BACKUP_LIST_TIME = @LAST_DATA_BACKUP_TIME
  END

  IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Last restore date for log files list: "'+CAST(ISNULL(@LOG_BACKUP_LIST_DATE, 0) AS NVARCHAR(80))+N' '+REPLICATE(N'0', 4 - LEN(CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80))))+CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80))+N'"')
SELECT 
			substring(@BACKUP_FOLDER_LOG,0,charindex('LOG',@BACKUP_FOLDER_LOG,0)+4) +
			  SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) + '\' +[subdirectory], 
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS [backup_date],
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS [backup_time],
			CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT) AS [compressed_flag]
		 FROM 
			#tblDir 
		 WHERE 
			[file] = 1
			AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
			AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
	  --   AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LOG_BACKUP_LIST_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80))))+CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80)) AS BIGINT)
			AND CHARINDEX('_LOG.',[subdirectory],1)>0
			 AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
		 ORDER BY
			[backup_date] ASC, [backup_time] ASC

IF (  @Is_Contains_Date = 0 ) 
BEGIN

	print 'With Date Folder'
		--With Date Folder
	  --now, gettting the actual list of log file for the restore...
		DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
		FOR
		 SELECT 
			substring(@BACKUP_FOLDER_LOG,0,charindex('LOG',@BACKUP_FOLDER_LOG,0)+4) +
			  SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) + '\' +[subdirectory], 
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS [backup_date],
		   SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS [backup_time],
			CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT) AS [compressed_flag]
		 FROM 
			#tblDir 
		 WHERE 
			[file] = 1
			AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
			AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
	  --   AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LOG_BACKUP_LIST_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80))))+CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80)) AS BIGINT)
			AND CHARINDEX('_LOG.',[subdirectory],1)>0
			  AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
		 ORDER BY
			[backup_date] ASC, [backup_time] ASC
		OPEN CUR
		FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
	  END
else
	  BEGIN
	  print 'Without Date Folder'
		--Without Date Folder
	  --now, gettting the actual list of log file for the restore...
		DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
		FOR
		 SELECT 
			@BACKUP_FOLDER_LOG+[subdirectory],
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8) AS [backup_date],
			SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS [backup_time],
			CAST((CASE CHARINDEX(N'.', REVERSE([subdirectory])) WHEN 0 THEN 0 ELSE CAST((CASE CHARINDEX(N'c', REVERSE(LOWER(SUBSTRING(REVERSE([subdirectory]), 1, CHARINDEX(N'.', REVERSE([subdirectory])) - 1))))  WHEN 0 THEN 0 ELSE 1 END) AS BIT) END) AS BIT) AS [compressed_flag]
		 FROM 
			#tblDir 
		 WHERE 
			[file] = 1
			AND LEFT([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 1) = REPLACE(@SOURCE_SERVERNAME, N'\', N'$')+N'_'+@SOURCE_DATABASE+N'_'
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)) = 1
			AND ISNUMERIC(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4)) = 1
			AND RIGHT([subdirectory], LEN(N'.standby')) != N'.standby'
	  --   AND CAST(SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2, 8)+SUBSTRING([subdirectory], LEN(REPLACE(@SOURCE_SERVERNAME, N'\', N'$')) + 1 + LEN(@SOURCE_DATABASE) + 2 + 8 + 1, 4) AS BIGINT) >= CAST(CAST(@LOG_BACKUP_LIST_DATE AS NVARCHAR(20))+REPLICATE(N'0', 4 - LEN(CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80))))+CAST(@LOG_BACKUP_LIST_TIME AS NVARCHAR(80)) AS BIGINT)
			AND CHARINDEX('_LOG.',[subdirectory],1)>0
			  AND CHARINDEX(@IGNORE_FILE_PATTERN, RTRIM(LTRIM([subdirectory]))) = 0
		 ORDER BY
			[backup_date] ASC, [backup_time] ASC
		OPEN CUR
		FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
	  END


  WHILE @@FETCH_STATUS = 0
  BEGIN
	  BEGIN TRY
   --Restore the log file to the database...

print '@LOG_BACKUP_FILE:  ' + @LOG_BACKUP_FILE
   --First - verIFying access to the backup file (so we can issue a better error msg IF necessary)
   PRINT(N' - VerIFying access to LOG backup file "'+ISNULL(@LOG_BACKUP_FILE, N'')+N'"...')
   SET @CMD = N'EXEC [master]..[xp_fileexist] N'''+ISNULL(@LOG_BACKUP_FILE, N'')+N''', @FILE_ACCESS_CHECK_OUT OUTPUT'
   EXEC master..sp_executesql @CMD, N'@FILE_ACCESS_CHECK_OUT INT OUTPUT', @FILE_ACCESS_CHECK_OUT = @FILE_ACCESS_CHECK OUTPUT
   IF @FILE_ACCESS_CHECK = 0
   BEGIN
	SET @ERR_MSG = N'Unable to access backup file "'+ISNULL(@LOG_BACKUP_FILE, N'')+N'"'
	RAISERROR(@ERR_MSG, 16, 1)
	GOTO FinishDrp
   END

print '@LOG_BACKUP_FILE' + @LOG_BACKUP_FILE

   --Now, the actual restore command...
   PRINT(N' - Restore database "'+ISNULL(@DESTINATION_DB_NAME, N'')+N'" from LOG backup "'+ISNULL(@LOG_BACKUP_FILE, N'')+N'"...')
   
   IF CHARINDEX(N'.bak', @LAST_DATA_BACKUP_FILE) > 0 SET @USE_NATIVE_RESTORE = 1
   print '@USE_NATIVE_RESTORE' + cast (@USE_NATIVE_RESTORE as nvarchar(100))
   SET @CMD = N'exec EZManagePro..SP_RESTORE @DATABASE_NAME = N'''+ISNULL(@DESTINATION_DB_NAME, N'')+N''', @BACKUP_TYPE = N''L'', @FILENAME = N'''+ISNULL(@LOG_BACKUP_FILE, N'')+N''', @KEEPOPEN = 1, @COMPRESSED = '+CAST(ISNULL(@LOG_BACKUP_COMPRESSED_FLAG, 1) AS NVARCHAR(20))+N', @NEW_DB_SOURCE_CMPT_LEVEL = '+CAST(ISNULL(@SOURCE_DATABASE_COMPATIBILITY_LEVEL, 90) AS NVARCHAR(20))+N', @KEEPOPEN_STANDBY = '+CAST(ISNULL(@RESTORE_WITH_STANDBY_FLAG, 0) AS NVARCHAR(20))
	  +N', @USE_NATIVE_RESTORE = ' + cast (@USE_NATIVE_RESTORE as nvarchar(1))
   IF @USR_BLOCKSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_BLOCKSIZE = '+CAST(ISNULL(@USR_BLOCKSIZE, 0) AS NVARCHAR(20))
   IF @USR_BUFFERCOUNT IS NOT NULL SET @CMD = @CMD+N', @USR_BUFFERCOUNT = '+CAST(ISNULL(@USR_BUFFERCOUNT, 0) AS NVARCHAR(20))
   IF @USR_MAXTRANSFERSIZE IS NOT NULL SET @CMD = @CMD+N', @USR_MAXTRANSFERSIZE = '+CAST(ISNULL(@USR_MAXTRANSFERSIZE, 0) AS NVARCHAR(20))  
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   SET @CMD_START=GETDATE()
   print @CMD
   EXEC master..sp_executesql @CMD
   SET @ERROR=@@ERROR
   IF  @ERROR >0 
	BEGIN 
	  SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	--RAISERROR (N'EZManage restore command %s failed after %s , with error number %d', -- Message text.
	--       16, -- Severity,
	--       1, -- State,
	--       @CMD, -- First argument.
	--           @CMD_DURATION,
	--       @ERROR) WITH LOG  
	
	END

  FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
  
	  END TRY
	  
  BEGIN CATCH
	 SELECT @CMD_DURATION=[dbo].[FUNC_MS_TO_TEXT](DATEDIFF(MS,@CMD_START,GETDATE())) 
	 FETCH NEXT FROM CUR INTO @LOG_BACKUP_FILE, @LOG_BACKUP_DATE, @LOG_BACKUP_TIME, @LOG_BACKUP_COMPRESSED_FLAG
	 print 'Error : ' + ERROR_MESSAGE()
--    CONTINUE
	
  END CATCH
  END
  

---------------------------------------------------------------------------------------------------------------------------------------------------------------
  --When finishing the sequence restore, completing the process.
  PRINT(N' - Log DRP process is completed.')
  GOTO FinishDrp
END
  ELSE
BEGIN
  --The database is not in "standby" nor "recovering" mode, which means that the log restore operation
  --cannot be executed...
  RAISERROR(N'Unable to apply log backups to a database that is not in "standby" nor "recovering" mode.', 16, 1)
  GOTO FinishDrp
END
END

FinishDrp:
--Cleaning...
DROP TABLE #tblDir