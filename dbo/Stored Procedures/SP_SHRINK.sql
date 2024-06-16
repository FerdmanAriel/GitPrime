--EZMANAGE_
create PROCEDURE [dbo].[SP_SHRINK]
-- Old name: EP_SHRINK
@DB_NAME NVARCHAR(128),
@SHRINK_LOG BIT = 0,
@SHRINK_DB BIT = 0,
@FREE_SPACE_PERCENT_DB INT = 20,
@DESIRED_LOG_SIZE_MB INT = 2,
@SHOW_MESSAGES_FLAG BIT = 0,
@SHRINK_LOG_WITH_TRUNCATE_ONLY BIT=0,
@TTL_EXPARATION INT = 5 
--WITH ENCRYPTION
AS

SET NOCOUNT ON 
IF OBJECT_ID('tempdb..#DBFiles') IS NOT NULL
   DROP TABLE #DBFiles

DECLARE 
    @FILENAME           sysname    ,
      @FILEGROUP_TYPE   NVARCHAR(4),
      @DATA        INT      ,
      @DATA_CAN_SHRINK INT          ,
      @LOG             INT          ,
      @LOG_CAN_SHRINK  INT          ,
      @CMD NVARCHAR(4000)    ,
      @BACKUP_LOG_NEED BIT 
      
         
CREATE TABLE #DBFiles (

      [FILENAME]        sysname           NOT NULL ,
      [FILEGROUP_TYPE]  NVARCHAR(4) NOT NULL ,
      [DATA]            INT         NULL ,
      [DATA_CAN_SHRINK] INT         NULL ,
      [LOG]       INT         NULL ,
      [LOG_CAN_SHRINK]        INT         NULL 
      
);
IF @SHRINK_LOG=1 
   CHECKPOINT;

  
IF @SHRINK_LOG_WITH_TRUNCATE_ONLY=1 AND    
   LEFT(CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(80)), CHARINDEX(N'.', CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(80))) - 1)<=9
BEGIN
  --In SQL 2000 and 2005 - we can execute a log truncation using the backup command
  SET @CMD = N'BACKUP LOG ['+@DB_NAME+N'] WITH TRUNCATE_ONLY'
  IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Truncating database log file...')
  EXEC master..sp_executesql @CMD
END   
   --------Gives the Log option to shrink
ELSE IF @SHRINK_LOG=1 AND DATABASEPROPERTYEX(@DB_NAME, N'Recovery')<>'SIMPLE'
    BEGIN 
        exec EZManagePro..SP_BACKUP @DATABASE_NAME = @DB_NAME, @BACKUP_TYPE = N'L', @LOCATION = N'', @COMPRESS = 1, @TTL = @TTL_EXPARATION, @COMPRESSION_LEVEL = 1, @ENCRYPTION_KEY = NULL, @FTP_LOCATION = NULL, @COPY_LOCATION = NULL, @SHOW_PROGRESS = 1, @USR_BLOCKSIZE = 0, @USR_BUFFERCOUNT = 0, @USR_MAXTRANSFERSIZE = 0, @BACKUP_TO = 0, @THREADS = 1, @INCLUDE_TIMESTAMP_IN_FILENAME = 1, @COPY_ONLY = 0, @WAIT_FOR_RUNNING_BACKUP_TO_FINISH = 1
        WAITFOR DELAY '00:00:05'
               
    END 

INSERT INTO #DBFiles
EXEC(
'SELECT     
      [FILENAME]        = a.name,
      [FILEGROUP_TYPE]  = case when a.groupid = 0 then ''Log'' else ''Data'' end,
      [DATA]               = case when a.groupid <> 0 then a.[fl_size] else 0 end,
      [DATA_CAN_SHRINK]       = ISNULL(case when a.groupid <> 0 then a.[fl_size] else 0 end -
                                       case when a.groupid <> 0 then a.[fl_unused] else 0 end,0),
      [LOG]             = case when a.groupid = 0 then a.[fl_size] else 0 end,
      [LOG_CAN_SHRINK] = ISNULL(case when a.groupid = 0 then a.[fl_size] else 0 end -
                                   case when a.groupid = 0 then a.[fl_unused] else 0 end,0)
from
      (
      Select
            aa.*,
            [FILEGROUP] = isnull(bb.groupname,''''),
            [fl_size]   = 
                  convert(int,round((aa.size*1.000)/128.000,0)),
            [fl_used]   =
                  convert(int,round(fileproperty(aa.name,''SpaceUsed'')/128.000,0)),
            [fl_unused] =
                  convert(int,round((aa.size-fileproperty(aa.name,''SpaceUsed''))/128.000,0))
      from
            ' +'[' + @DB_NAME + '].dbo.sysfiles as aa
            left join
            ' +'[' + @DB_NAME + '].dbo.sysfilegroups as bb
            on  aa.groupid = bb.groupid 
      ) as a
      ')
      
     IF @SHOW_MESSAGES_FLAG = 1 
        SELECT FILENAME,FILEGROUP_TYPE,DATA,DATA_CAN_SHRINK,LOG,LOG_CAN_SHRINK
            FROM   #DBFiles
    DECLARE CUR_DATA CURSOR LOCAL FORWARD_ONLY READ_ONLY
    FOR 
    SELECT FILENAME,FILEGROUP_TYPE,DATA,DATA_CAN_SHRINK,LOG,LOG_CAN_SHRINK
    FROM   #DBFiles

	select * from #DBFiles
    
    OPEN CUR_DATA
FETCH NEXT FROM CUR_DATA INTO 
      @FILENAME        ,            
      @FILEGROUP_TYPE   ,
      @DATA        ,
      @DATA_CAN_SHRINK ,
      @LOG             ,
      @LOG_CAN_SHRINK  
      
 
 WHILE @@FETCH_STATUS = 0
BEGIN
  IF @SHRINK_DB = 1 and @FILEGROUP_TYPE='Data' and @DATA > 10
  BEGIN
  
        SET @CMD = N'USE ['+@DB_NAME+N'] DBCC SHRINKFILE (N'''+RTRIM(LTRIM(@FILENAME))+N''''
        IF @DATA*@FREE_SPACE_PERCENT_DB/100>@DATA_CAN_SHRINK
            SET @DATA_CAN_SHRINK=@DATA*@FREE_SPACE_PERCENT_DB/100
        
        SET @CMD = @CMD+N', '+CAST(@DATA_CAN_SHRINK AS NVARCHAR(80))+N')'
        IF @SHOW_MESSAGES_FLAG = 1 
              BEGIN
              PRINT (N' - Shrinking database DATA file '+@FILENAME+N'...')
              PRINT @CMD
              END
        EXEC master..sp_executesql @CMD
        
  END 
  ELSE IF @SHRINK_LOG=1  and  @FILEGROUP_TYPE='Log' and @LOG > 10
  BEGIN
   
        SET @CMD = N'USE ['+@DB_NAME+N'] DBCC SHRINKFILE (N'''+RTRIM(LTRIM(@FILENAME))+N''''
          IF  @DESIRED_LOG_SIZE_MB>@LOG_CAN_SHRINK
                SET @LOG_CAN_SHRINK=@DESIRED_LOG_SIZE_MB
        
          SET @CMD = @CMD+N', '+CAST(@LOG_CAN_SHRINK AS NVARCHAR(80))+N')'
          IF @SHOW_MESSAGES_FLAG = 1 
               BEGIN
               PRINT (N' - Shrinking database Log file '+@FILENAME+N'...')
               PRINT @CMD
               END
       IF @SHOW_MESSAGES_FLAG = 1 
       PRINT @CMD 
       EXEC master..sp_executesql @CMD
        
        
   END

  
  FETCH NEXT FROM CUR_DATA INTO 
      @FILENAME        ,            
      @FILEGROUP_TYPE   ,
      @DATA        ,
      @DATA_CAN_SHRINK ,
      @LOG             ,
      @LOG_CAN_SHRINK 


END

CLOSE CUR_DATA
DEALLOCATE CUR_DATA

IF OBJECT_ID('tempdb..#DBFiles') IS NOT NULL
   DROP TABLE #DBFiles