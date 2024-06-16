--EZMANAGE_
create PROCEDURE [dbo].[SP_REINDEX] 
-- test func: 3
@DB_NAME NVARCHAR(128),
@WITH_REBUILD BIT = 1,
@REBUILD_FILLFACTOR INT = 80,
@WITH_SIMPLE_RECOVERY BIT = 0,
--@SCAN_DENSITY_MARGIN INT = 85, (removed by Michal)
@SHRINK_DB_WHEN_FINISHED BIT = 0,
@FragmentationThresholdForReorganizeTableLowerLimit VARCHAR(10) = '10.0', -- Percent (Added by Michal)
@FragmentationThresholdForRebuildTableLowerLimit VARCHAR(10) = '30.0', -- Percent (Added by Michal)
--@RowNumber INT,--Added by Michal
@BACKUP_LOG BIT = 1,
@SHOW_MESSAGE_FLAG BIT = 1

AS

----------------------------------------------------------------
--Validating user-defined variables...
SET XACT_ABORT ON
SET NOCOUNT ON

DECLARE @SQLversion nvarchar(128)
SET @SQLversion = CAST(serverproperty('ProductVersion') AS nvarchar)
SET @SQLversion = SUBSTRING(@SQLversion, 1, CHARINDEX('.', @SQLversion) - 1)
 
IF (@SQLversion <> 8) 
BEGIN 

--IF @SCAN_DENSITY_MARGIN IS NULL SET @SCAN_DENSITY_MARGIN = 85 (removed by Michal)
IF @FragmentationThresholdForReorganizeTableLowerLimit IS NULL SET @FragmentationThresholdForReorganizeTableLowerLimit = '10.0' -- Added by Michal
IF @FragmentationThresholdForRebuildTableLowerLimit IS NULL SET @FragmentationThresholdForRebuildTableLowerLimit = '30.0' --Added by Michal
IF @REBUILD_FILLFACTOR IS NULL SET @REBUILD_FILLFACTOR = 0

IF @SHOW_MESSAGE_FLAG = 1
BEGIN
PRINT (N' - Starting index optimization...')
PRINT (N' -    Database name: '+@DB_NAME)
--PRINT (N' -    Scan density margin: '+CAST(@SCAN_DENSITY_MARGIN AS NVARCHAR(20))) (removed by Michal)
PRINT (N' -    Fragmentation margin is between : '+CAST(@FragmentationThresholdForReorganizeTableLowerLimit AS NVARCHAR(20))) + N'and ' + @FragmentationThresholdForRebuildTableLowerLimit --Added by Michal 
 PRINT (N' -    Rebuild fill factor: '+CAST(ISNULL(@REBUILD_FILLFACTOR, 0) AS NVARCHAR(20)))
END

----------------------------------------------------------------
--Declaring local variables
DECLARE @CMD NVARCHAR(3200)
DECLARE @TABLE_NAME NVARCHAR(256)
DECLARE @INDEX_NAME NVARCHAr(512)
DECLARE @INDEX_ID INT
DECLARE @TABLE_SCHEMA NVARCHAR(128)
DECLARE @TYPE INT
--DECLARE @SCAN_DENSITY FLOAT (removed by Michal)
DECLARE @OPTIMIZATION_COUNT INT
DECLARE @RECOVERY_MODEL_HOLDER AS NVARCHAR(128)
DECLARE @AvgFragmentationInPercent DECIMAL

----------------------------------------------------------------
--Performing preleminary operations...
SET @OPTIMIZATION_COUNT = 0
----------------------------------------------------------------

--Temporary table to hold the indexes on the database
CREATE TABLE #tblIndexes (
[TableName] NVARCHAR(256),
[IndexName] NVARCHAR(512),
[IndexId] INT,
[TableSchema] NVARCHAR(128),
[Type] int
)

--Temporary table to hold the information from the "show contig" command
CREATE TABLE #tblShowContig (
[ObjectName] NVARCHAR(128),
[ObjectId] BIGINT,
[IndexName] NVARCHAR(512),
[IndexId] INT,
[Level] INT,
[Pages] INT,
[Rows] INT,
[MinimumRecordSize] INT,
[MaximumRecordSize] INT,
[AverageRecordSize] FLOAT,
[ForwardedRecords] INT,
[Extents] INT,
[ExtentSwitches] INT,
[AverageFreeBytes] FLOAT,
[AveragePageDensity] FLOAT,
[ScanDensity] FLOAT,
[BestCount] INT,
[ActualCount] INT,
[LogicalFragmentation] FLOAT,
[ExtentFragmentation] FLOAT
)

--First we get all the indexes in the database, we'll take only the indexes that are NOT statistics and NOT on system tables
IF EXISTS(SELECT * FROM master..sysdatabases WHERE name=@DB_NAME AND cmptlevel<90)
BEGIN
               SET @CMD = N'USE ['+@DB_NAME+N'] SELECT obj.[name] AS [TableName], ind.[name] AS [IndexName], ind.[indid] AS [IndexId], inf.[TABLE_SCHEMA] AS [TableSchema] ,0 as Type
                FROM sysindexes ind INNER JOIN sysobjects obj ON ind.[id] = obj.[id]
                INNER JOIN [INFORMATION_SCHEMA].[TABLES] inf ON obj.[name] = inf.[TABLE_NAME] and ( ( obj.[type] = ''U'' and inf.TABLE_TYPE = ''BASE TABLE'' ) or ( obj.[type] = ''V'' and inf.TABLE_TYPE = ''VIEW'' ) )
                           inner join sys.indexes sidx on sidx.index_id=ind.id and sidx.object_id=ind.id
                WHERE ind.[name] IS NOT NULL AND ind.[name] != N'''' AND ind.[indid] > 0 AND ind.[indid] < 255 AND obj.[xtype] != N''S'' AND INDEXPROPERTY(obj.[id], ind.[name], N''IsStatistics'') = 0 
          and sidx.is_disabled = 0
                 AND NOT (ind.status & 64 = 0 AND ind.status & 32 = 32)  --DTA STATISTIC 
				 	
                ORDER BY obj.[name] ASC, ind.[name] ASC, ind.[indid] ASC'
END 
ELSE 
BEGIN 
              SET @CMD = N'USE ['+@DB_NAME+N']
                 SELECT obj.[name] AS [TableName], ind.[name] AS [IndexName], ind.[indid] AS [IndexId], inf.[TABLE_SCHEMA] AS [TableSchema], i2.type as Type
                 FROM sysindexes ind INNER JOIN sys.objects obj ON ind.[id] = obj.[object_id]
                INNER JOIN [INFORMATION_SCHEMA].[TABLES] inf ON obj.[name] = inf.[TABLE_NAME] and
                                                                inf.[TABLE_SCHEMA]=SCHEMA_NAME(obj.schema_id) AND( ( obj.[type] = ''U'' and inf.TABLE_TYPE = ''BASE TABLE'' ) or ( obj.[type] = ''V'' and inf.TABLE_TYPE = ''VIEW'' ) )
                inner join sys.indexes i2 on (i2.object_id = ind.id and i2.name=ind.name)                                    
                 AND NOT (ind.status & 64 = 0 AND ind.status & 32 = 32)  --DTA STATISTIC
                ORDER BY obj.[name] ASC, ind.[name] ASC, ind.[indid] ASC'

                PRINT @CMD
END


INSERT INTO #tblIndexes EXEC master..sp_executesql @CMD
IF @SHOW_MESSAGE_FLAG = 1 SELECT * FROM #tblIndexes

--This cursor will go over all the indexes and fill the show contig table

DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
FOR
  SELECT DISTINCT [TableName], [IndexName], [IndexId], [TableSchema],[Type] FROM #tblIndexes
OPEN CUR
FETCH NEXT FROM CUR INTO @TABLE_NAME, @INDEX_NAME, @INDEX_ID, @TABLE_SCHEMA,@TYPE
WHILE @@FETCH_STATUS = 0
BEGIN
  --Populating the temporary "show contig" table
  BEGIN TRY

  BEGIN TRAN
   SET @CMD = N'USE ['+@DB_NAME+N'] DBCC SHOWCONTIG (N''['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']'', '+CAST(@INDEX_ID AS NVARCHAR(20))+N') WITH FAST, TABLERESULTS, NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1
      PRINT @CMD
   
	INSERT INTO #tblShowContig EXEC master..sp_executesql @CMD
    COMMIT TRAN
   END TRY
   BEGIN CATCH
    ROLLBACK TRAN
   END CATCH

FETCH NEXT FROM CUR INTO @TABLE_NAME, @INDEX_NAME, @INDEX_ID, @TABLE_SCHEMA,@TYPE
END
CLOSE CUR
DEALLOCATE CUR
-----------------------------------

IF @SHOW_MESSAGE_FLAG = 1 SELECT * FROM #tblShowContig

/*
--If there are any indexes to optimize, we check if the recovery model should be changed...
IF EXISTS (SELECT [ObjectName], [IndexName], [LogicalFragmentation] FROM #tblShowContig WHERE [LogicalFragmentation] >= @FragmentationThresholdForReorganizeTableLowerLimit)
BEGIN
  IF @WITH_SIMPLE_RECOVERY = 1
  BEGIN
   SET @RECOVERY_MODEL_HOLDER = (SELECT CAST(DATABASEPROPERTYEX (@DB_NAME, N'Recovery') AS NVARCHAR))
   IF @RECOVERY_MODEL_HOLDER <> N'SIMPLE'
   BEGIN
    SET @CMD = N'ALTER DATABASE ['+@DB_NAME+'] SET RECOVERY SIMPLE'
    EXEC master..sp_executesql @CMD
    PRINT N' - Database recovery changed to "SIMPLE"'
   END
  END
END
*/
-----------------------------------
--This cursor goes over the relevant indexes and rebuild/defrag them...
DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
FOR
SELECT [ObjectName], [IndexName], [LogicalFragmentation] FROM #tblShowContig WHERE [LogicalFragmentation] >= @FragmentationThresholdForReorganizeTableLowerLimit
OPEN CUR
FETCH NEXT FROM CUR INTO @TABLE_NAME, @INDEX_NAME, @AvgFragmentationInPercent
WHILE @@FETCH_STATUS = 0
BEGIN
  -----------------------------------
  SELECT TOP 1 @TABLE_SCHEMA = [TableSchema], @TYPE = [Type] FROM #tblIndexes WHERE [TableName] = @TABLE_NAME AND [IndexName] = @INDEX_NAME

  /*
  IF @WITH_REBUILD = 1
  BEGIN
   --This means we need to REBUILD the index...
   --Before we can rebuild it - we need to the the schema of that index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Rebuild index "['+@DB_NAME+N'].['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']" with fill factor '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N'...')
   SET @CMD = N'USE ['+@DB_NAME+N'] DBCC DBREINDEX (N''['+@DB_NAME+N'].['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']'', ['+@INDEX_NAME+N'], '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N') WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   EXEC master..sp_executesql @CMD
   SET @OPTIMIZATION_COUNT = @OPTIMIZATION_COUNT + 1
  END
   ELSE
  BEGIN
   --This means we only need to DEFRAG the index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Defrag index "['+@DB_NAME+N'].['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']"...')
   SET @CMD = N'USE ['+@DB_NAME+N'] DBCC INDEXDEFRAG (['+@DB_NAME+N'], N''['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']'', ['+@INDEX_NAME+N']) WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   EXEC master..sp_executesql @CMD
   SET @OPTIMIZATION_COUNT = @OPTIMIZATION_COUNT + 1
  END
  */

  --Reorganizing the index 
  IF((@AvgFragmentationInPercent >= @FragmentationThresholdForReorganizeTableLowerLimit) AND (@AvgFragmentationInPercent < @FragmentationThresholdForRebuildTableLowerLimit))
  BEGIN
   --This means we only need to DEFRAG the index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Defrag index "['+@DB_NAME+N'].['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']"...')
   SET @CMD = N'USE ['+@DB_NAME+N'] DBCC INDEXDEFRAG (['+@DB_NAME+N'], N''['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']'', ['+@INDEX_NAME+N']) WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   EXEC master..sp_executesql @CMD
   SET @OPTIMIZATION_COUNT = @OPTIMIZATION_COUNT + 1 
  END 
  --PRINT(N' - Index optimization completed, '+CAST(@OPTIMIZATION_COUNT AS NVARCHAR(20))+N' indexes were rebuilt')

  --Rebuilding the index 
  ELSE IF (@AvgFragmentationInPercent >= @FragmentationThresholdForRebuildTableLowerLimit ) 
  BEGIN
   --This means we need to REBUILD the index...
   --Before we can rebuild it - we need to the the schema of that index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Rebuild index "['+@DB_NAME+N'].['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']" with fill factor '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N'...')

   SET @CMD = N'USE ['+@DB_NAME+N'] DBCC DBREINDEX (N''['+@DB_NAME+N'].['+@TABLE_SCHEMA+N'].['+@TABLE_NAME+N']'', ['+@INDEX_NAME+N'] '
   IF (@TYPE IN (5,6))
		SET @CMD = @CMD +N') WITH NO_INFOMSGS'
	else
	SET @CMD = @CMD+ ',' +CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N') WITH NO_INFOMSGS'
   
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD)
   EXEC master..sp_executesql @CMD
   SET @OPTIMIZATION_COUNT = @OPTIMIZATION_COUNT + 1  

                IF @BACKUP_LOG = 1
                BEGIN   
                   DECLARE @LogSize INT
                   SET @CMD = 'SELECT @LogSize = MAX(size*8/1024) FROM [' + @DB_NAME + '].sys.database_files WHERE   data_space_id = 0'
                   EXEC master..sp_executesql @CMD,
                   N'@LogSize int OUTPUT', 
                   @LogSize=@LogSize OUTPUT
                      
                   SELECT @LogSize
                                IF (@LogSize > 2048)
                                   PRINT 'michal'
                --   BEGIN
                                -- SET @CMD = 'EZManagePro..SP_BACKUP @DATABASE_NAME = N''['+@DB_NAME+N']'', @BACKUP_TYPE = N''L'', @LOCATION = N'', @COMPRESS = 1, @TTL = 5, @COMPRESSION_LEVEL = 1, @ENCRYPTION_KEY = NULL, @FTP_LOCATION = NULL, @COPY_LOCATION = NULL, @SHOW_PROGRESS = 0, @USR_BLOCKSIZE = 65536, @USR_BUFFERCOUNT = 20, @USR_MAXTRANSFERSIZE = 1048576, @BACKUP_TO = 0, @THREADS = 1, @TSMCLASS = N''TRUE'', @INCLUDE_TIMESTAMP_IN_FILENAME = 1, @COPY_ONLY = 0, @WAIT_FOR_RUNNING_BACKUP_TO_FINISH = 0'
                --    PRINT @CMD
                                --END
                     END
                END
  --PRINT(N' - Index optimization completed, '+CAST(@OPTIMIZATION_COUNT AS NVARCHAR(20))+N' indexes were defragged')
   
  -----------------------------------
FETCH NEXT FROM CUR INTO @TABLE_NAME, @INDEX_NAME, @AvgFragmentationInPercent
END
CLOSE CUR
DEALLOCATE CUR

-----------------------------------
/*
--If there WERE any indexes to optimize, we check if the recovery model should be changed back...
IF EXISTS (SELECT [ObjectName], [IndexName], [LogicalFragmentation] FROM #tblShowContig WHERE [LogicalFragmentation] >= @FragmentationThresholdForReorganizeTableLowerLimit)
BEGIN
  IF @WITH_SIMPLE_RECOVERY = 1
  BEGIN
   IF @RECOVERY_MODEL_HOLDER <> N'SIMPLE'
   BEGIN
    SET @CMD = N'ALTER DATABASE ['+@DB_NAME+'] SET RECOVERY '+@RECOVERY_MODEL_HOLDER
    EXEC master..sp_executesql @CMD
    PRINT N' - Database recovery changed to "'+@RECOVERY_MODEL_HOLDER+'"'
   END
  END
  */

  --Now, after the operation basically completed, we check if a shrink operation is required...
  IF @SHRINK_DB_WHEN_FINISHED = 1
  BEGIN
   --A shrink operation is required on the database...
   --The shrink operation will apply ONLY to the log...
   PRINT(N' - Shrinking database log...')
   EXEC [EZManagePro]..[SP_SHRINK] @DB_NAME, 1, 0
  END
--END
-----------------------------------------------------------
--END
-----------------------------------
DROP TABLE #tblIndexes
DROP TABLE #tblShowContig
END

--------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------SQL 2000-------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------

ELSE IF (@SQLversion = 8)
BEGIN
 DECLARE @SCAN_DENSITY_MARGIN INT 
 SET @SCAN_DENSITY_MARGIN  = 85
 
----------------------------------------------------------------
--Validating user-defined variables...

IF @SCAN_DENSITY_MARGIN IS NULL SET @SCAN_DENSITY_MARGIN = 85
IF @REBUILD_FILLFACTOR IS NULL SET @REBUILD_FILLFACTOR = 0

IF @SHOW_MESSAGE_FLAG = 1
BEGIN
 PRINT (N' - Starting index optimization...')
 PRINT (N' -    Database name: '+@DB_NAME)
 PRINT (N' -    Scan density margin: '+CAST(@SCAN_DENSITY_MARGIN AS NVARCHAR(20)))
 PRINT (N' -    Rebuild fill factor: '+CAST(ISNULL(@REBUILD_FILLFACTOR, 0) AS NVARCHAR(20)))
END

----------------------------------------------------------------
--Declaring local variables
DECLARE @CMD2000 NVARCHAR(3200)
DECLARE @TABLE_NAME2000 NVARCHAR(256)
DECLARE @INDEX_NAME2000 NVARCHAr(512)
DECLARE @INDEX_ID2000 INT
DECLARE @TABLE_SCHEMA2000 NVARCHAR(128)
DECLARE @SCAN_DENSITY2000 FLOAT
DECLARE @OPTIMIZATION_COUNT2000 INT
DECLARE @RECOVERY_MODEL_HOLDER2000 AS NVARCHAR(128)

----------------------------------------------------------------
--Performing preleminary operations...
SET @OPTIMIZATION_COUNT2000 = 0
----------------------------------------------------------------

--Temporary table to hold the indexes on the database
CREATE TABLE #tblIndexes2000 (
 [TableName] NVARCHAR(256),
 [IndexName] NVARCHAR(512),
 [IndexId] INT,
 [TableSchema] NVARCHAR(128)
)

--Temporary table to hold the information from the "show contig" command
CREATE TABLE #tblShowContig2000 (
 [ObjectName] NVARCHAR(128),
 [ObjectId] BIGINT,
 [IndexName] NVARCHAR(512),
 [IndexId] INT,
 [Level] INT,
 [Pages] INT,
 [Rows] INT,
 [MinimumRecordSize] INT,
 [MaximumRecordSize] INT,
 [AverageRecordSize] FLOAT,
 [ForwardedRecords] INT,
 [Extents] INT,
 [ExtentSwitches] INT,
 [AverageFreeBytes] FLOAT,
 [AveragePageDensity] FLOAT,
 [ScanDensity] FLOAT,
 [BestCount] INT,
 [ActualCount] INT,
 [LogicalFragmentation] FLOAT,
 [ExtentFragmentation] FLOAT
)

--First we get all the indexes in the database, we'll take only the indexes that are NOT statistics and NOT on system tables
IF EXISTS(SELECT * FROM master..sysdatabases WHERE name=@DB_NAME AND cmptlevel<90)
BEGIN
	SET @CMD2000 = N'USE ['+@DB_NAME+N'] SELECT obj.[name] AS [TableName], ind.[name] AS [IndexName], ind.[indid] AS [IndexId], usr.[name] AS [TableSchema] 
       FROM sysobjects obj INNER JOIN sysusers usr
      ON obj.[uid] = usr.[uid] 
       INNER JOIN sysindexes ind ON ind.[id] = obj.[id]
      INNER JOIN [INFORMATION_SCHEMA].[TABLES] inf ON obj.[name] = inf.[TABLE_NAME] 
       and ( ( obj.[type] = ''U'' and inf.TABLE_TYPE = ''BASE TABLE'' ) or ( obj.[type] = ''V'' and inf.TABLE_TYPE = ''VIEW'' ) )
      WHERE ind.[name] IS NOT NULL AND ind.[name] != N'''' AND ind.[indid] > 0 AND ind.[indid] < 255 AND obj.[xtype] != N''S'' AND INDEXPROPERTY(obj.[id], ind.[name], N''IsStatistics'') = 0 
           AND NOT (ind.status & 64 = 0 AND ind.status & 32 = 32)  --DTA STATISTIC
      ORDER BY obj.[name] ASC, ind.[name] ASC, ind.[indid] ASC'
END 
ELSE 
BEGIN 
	 SET @CMD2000 = N'USE ['+@DB_NAME+N'] 
	 SELECT obj.[name] AS [TableName], ind.[name] AS [IndexName], ind.[indid] AS [IndexId], inf.[TABLE_SCHEMA] AS [TableSchema] 
	 FROM sysindexes ind INNER JOIN sys.objects obj ON ind.[id] = obj.[object_id]
	 INNER JOIN [INFORMATION_SCHEMA].[TABLES] inf ON obj.[name] = inf.[TABLE_NAME] and 
				inf.[TABLE_SCHEMA]=SCHEMA_NAME(obj.schema_id) AND( ( obj.[type] = ''U'' and inf.TABLE_TYPE = ''BASE TABLE'' ) or ( obj.[type] = ''V'' and inf.TABLE_TYPE = ''VIEW'' ) )
	 WHERE ind.[name] IS NOT NULL AND ind.[name] != N'''' AND ind.[indid] > 0 AND ind.[indid] < 255 AND obj.[type] != N''S'' AND INDEXPROPERTY(obj.[object_id], ind.[name], N''IsStatistics'') = 0 
           AND NOT (ind.status & 64 = 0 AND ind.status & 32 = 32) 

	 ORDER BY obj.[name] ASC, ind.[name] ASC, ind.[indid] ASC'

	 PRINT @CMD2000
END
INSERT INTO #tblIndexes2000 EXEC master..sp_executesql @CMD2000
IF @SHOW_MESSAGE_FLAG = 1 SELECT * FROM #tblIndexes2000

IF @SCAN_DENSITY_MARGIN = 0
BEGIN
 -----------------------------------------------------------
 --Scan density is 0, we'll ignore it and rebuild/defrag all indexes in the database
 -----------------------------------------------------------
 IF @WITH_SIMPLE_RECOVERY = 1
 BEGIN
  SET @RECOVERY_MODEL_HOLDER2000 = (SELECT CAST(DATABASEPROPERTYEX (@DB_NAME, N'Recovery') AS NVARCHAR))
  IF @RECOVERY_MODEL_HOLDER2000 <> N'SIMPLE'
  BEGIN
   SET @CMD2000 = N'ALTER DATABASE ['+@DB_NAME+'] SET RECOVERY SIMPLE'
   EXEC master..sp_executesql @CMD2000
   PRINT N' - Database recovery changed to "SIMPLE"'
  END
 END

 --This cursor goes over ALL indexes and rebuild/defrag them...
 DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
 FOR
  SELECT [TableName], [IndexName], [TableSchema] FROM #tblIndexes2000
 OPEN CUR
 FETCH NEXT FROM CUR INTO @TABLE_NAME2000, @INDEX_NAME, @TABLE_SCHEMA2000
 WHILE @@FETCH_STATUS = 0
 BEGIN
  -----------------------------------
  IF @WITH_REBUILD = 1
  BEGIN
   --This means we need to REBUILD the index...
   --Before we can rebuild it - we need to the the schema of that index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Rebuild index "['+@DB_NAME+N'].['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']" with fill factor '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N'...')
   SET @CMD2000 = N'USE ['+@DB_NAME+N'] DBCC DBREINDEX (N''['+@DB_NAME+N'].['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']'', ['+@INDEX_NAME+N'], '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N') WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD2000)
   EXEC master..sp_executesql @CMD2000
   SET @OPTIMIZATION_COUNT2000 = @OPTIMIZATION_COUNT2000 + 1
  END
   ELSE
  BEGIN
   --This means we only need to DEFRAG the index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Defrag index "['+@DB_NAME+N'].['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']"...')
   SET @CMD2000 = N'USE ['+@DB_NAME+N'] DBCC INDEXDEFRAG (['+@DB_NAME+N'], N''['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']'', ['+@INDEX_NAME+N']) WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD2000)
   EXEC master..sp_executesql @CMD2000
   SET @OPTIMIZATION_COUNT2000 = @OPTIMIZATION_COUNT2000 + 1
  END
  -----------------------------------
 FETCH NEXT FROM CUR INTO @TABLE_NAME2000, @INDEX_NAME, @TABLE_SCHEMA2000
 END
 CLOSE CUR
 DEALLOCATE CUR
 
 PRINT(N' - All index optimization completed, '+CAST(@OPTIMIZATION_COUNT2000 AS NVARCHAR(20))+N' indexes were '+(CASE @WITH_REBUILD WHEN 1 THEN N'rebuilt' ELSE N'defragged' END))

 IF @WITH_SIMPLE_RECOVERY = 1
 BEGIN
  IF @RECOVERY_MODEL_HOLDER2000 <> N'SIMPLE'
  BEGIN
   SET @CMD2000 = N'ALTER DATABASE ['+@DB_NAME+'] SET RECOVERY '+@RECOVERY_MODEL_HOLDER2000
   EXEC master..sp_executesql @CMD2000
   PRINT N' - Database recovery changed to "'+@RECOVERY_MODEL_HOLDER2000+'"'
  END
 END

 --Now, after the operation basically completed, we check if a shrink operation is required...
 IF @SHRINK_DB_WHEN_FINISHED = 1
 BEGIN
  --A shrink operation is required on the database...
  --The shrink operation will apply ONLY to the log...
  PRINT(N' - Shrinking database log...')
  EXEC [EZManagePro]..[SP_SHRINK] @DB_NAME, 1, 0
 END
 -----------------------------------------------------------
END
 ELSE
BEGIN
 -----------------------------------------------------------
 --Rebuild/defrag should occur according to the scan density margin
 -----------------------------------------------------------
 --This cursor will go over all the indexes and fill the show contig table
 DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
 FOR
  SELECT DISTINCT [TableName], [IndexName], [IndexId], [TableSchema] FROM #tblIndexes2000
 OPEN CUR
 FETCH NEXT FROM CUR INTO @TABLE_NAME2000, @INDEX_NAME, @INDEX_ID2000, @TABLE_SCHEMA2000
 WHILE @@FETCH_STATUS = 0
 BEGIN

  --Populating the temporary "show contig" table
  
   SET @CMD2000 = N'USE ['+@DB_NAME+N'] DBCC SHOWCONTIG (N''['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']'', '+CAST(@INDEX_ID2000 AS NVARCHAR(20))+N') WITH FAST, TABLERESULTS, NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1
      PRINT @CMD2000

   INSERT INTO #tblShowContig2000 EXEC master..sp_executesql @CMD2000

  

 FETCH NEXT FROM CUR INTO @TABLE_NAME2000, @INDEX_NAME, @INDEX_ID2000, @TABLE_SCHEMA2000
 END
 CLOSE CUR
 DEALLOCATE CUR
 -----------------------------------

 IF @SHOW_MESSAGE_FLAG = 1 SELECT * FROM #tblShowContig2000

 --If there are any indexes to optimize, we check if the recovery model should be changed...
 IF EXISTS (SELECT [ObjectName], [IndexName], [ScanDensity] FROM #tblShowContig2000 WHERE [ScanDensity] <= @SCAN_DENSITY_MARGIN)
 BEGIN
  IF @WITH_SIMPLE_RECOVERY = 1
  BEGIN
   SET @RECOVERY_MODEL_HOLDER2000 = (SELECT CAST(DATABASEPROPERTYEX (@DB_NAME, N'Recovery') AS NVARCHAR))
   IF @RECOVERY_MODEL_HOLDER2000 <> N'SIMPLE'
   BEGIN
    SET @CMD2000 = N'ALTER DATABASE ['+@DB_NAME+'] SET RECOVERY SIMPLE'
    EXEC master..sp_executesql @CMD2000
    PRINT N' - Database recovery changed to "SIMPLE"'
   END
  END
 END
 -----------------------------------
 --This cursor goes over the relevant indexes and rebuild/defrag them...
 DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
 FOR
 SELECT [ObjectName], [IndexName], [ScanDensity] FROM #tblShowContig2000 WHERE [ScanDensity] <= @SCAN_DENSITY_MARGIN
 OPEN CUR
 FETCH NEXT FROM CUR INTO @TABLE_NAME2000, @INDEX_NAME, @SCAN_DENSITY2000
 WHILE @@FETCH_STATUS = 0
 BEGIN
  -----------------------------------
  SELECT TOP 1 @TABLE_SCHEMA2000 = [TableSchema] FROM #tblIndexes2000 WHERE [TableName] = @TABLE_NAME2000 AND [IndexName] = @INDEX_NAME

  IF @WITH_REBUILD = 1
  BEGIN
   --This means we need to REBUILD the index...
   --Before we can rebuild it - we need to the the schema of that index...
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Rebuild index "['+@DB_NAME+N'].['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']" with fill factor '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N'...')
   SET @CMD2000 = N'USE ['+@DB_NAME+N'] DBCC DBREINDEX (N''['+@DB_NAME+N'].['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']'', ['+@INDEX_NAME+N'], '+CAST(@REBUILD_FILLFACTOR AS NVARCHAR(20))+N') WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD2000)
   EXEC master..sp_executesql @CMD2000
   SET @OPTIMIZATION_COUNT2000 = @OPTIMIZATION_COUNT2000 + 1
  END
   ELSE
  BEGIN
   --This means we only need to DEFRAG the index...
   
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Defrag index "['+@DB_NAME+N'].['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']"...')
   SET @CMD2000 = N'USE ['+@DB_NAME+N'] DBCC INDEXDEFRAG (['+@DB_NAME+N'], N''['+@TABLE_SCHEMA2000+N'].['+@TABLE_NAME2000+N']'', ['+@INDEX_NAME+N']) WITH NO_INFOMSGS'
   IF @SHOW_MESSAGE_FLAG = 1 PRINT (N' - Command: '+@CMD2000)
   EXEC master..sp_executesql @CMD2000
   SET @OPTIMIZATION_COUNT2000 = @OPTIMIZATION_COUNT2000 + 1
  END
  -----------------------------------
 FETCH NEXT FROM CUR INTO @TABLE_NAME2000, @INDEX_NAME, @SCAN_DENSITY2000
 END
 CLOSE CUR
 DEALLOCATE CUR
 
 PRINT(N' - Index optimization completed, '+CAST(@OPTIMIZATION_COUNT2000 AS NVARCHAR(20))+N' indexes were '+(CASE @WITH_REBUILD WHEN 1 THEN N'rebuilt' ELSE N'defragged' END))

 -----------------------------------
 --If there WERE any indexes to optimize, we check if the recovery model should be changed back...
 IF EXISTS (SELECT [ObjectName], [IndexName], [ScanDensity] FROM #tblShowContig2000 WHERE [ScanDensity] <= @SCAN_DENSITY_MARGIN)
 BEGIN
  IF @WITH_SIMPLE_RECOVERY = 1
  BEGIN
   IF @RECOVERY_MODEL_HOLDER2000 <> N'SIMPLE'
   BEGIN
    SET @CMD2000 = N'ALTER DATABASE ['+@DB_NAME+'] SET RECOVERY '+@RECOVERY_MODEL_HOLDER2000
    EXEC master..sp_executesql @CMD2000
    PRINT N' - Database recovery changed to "'+@RECOVERY_MODEL_HOLDER2000+'"'
   END
  END

  --Now, after the operation basically completed, we check if a shrink operation is required...
  IF @SHRINK_DB_WHEN_FINISHED = 1
  BEGIN
   --A shrink operation is required on the database...
   --The shrink operation will apply ONLY to the log...
   PRINT(N' - Shrinking database log...')
   EXEC [EZManagePro]..[SP_SHRINK] @DB_NAME, 1, 0
  END
 END
 -----------------------------------------------------------
END
-----------------------------------
DROP TABLE #tblIndexes2000
DROP TABLE #tblShowContig2000
END