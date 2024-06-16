--EZMANAGE_
create PROCEDURE [dbo].[SP_BACKUP_REMOVED_EXPIRED]
-- Old name: EZME_EXPIRED_BACKUP
 @REMOVE_EXPIRED_PHYSICAL BIT = NULL,
 @REMOVE_EXPIRED_HISTORY BIT = NULL,
 @USE_EXTENDED BIT = NULL,
 @OVERRIDE_Server_Settings BIT = NULL,
 @SHOW_MESSAGES_FLAG BIT = 1
--WITH ENCRYPTION
AS

SET NOCOUNT ON

DECLARE @BACKUP_SET_ID INT
DECLARE @MEDIA_SET_ID INT
DECLARE @PHYSICAL_DEVICE_NAME NVARCHAR(1024)
DECLARE @DIRECTORY NVARCHAR(1024)
DECLARE @CMDSHELLFLAG BIT
DECLARE @CMD_PHYSICAL NVARCHAR(512)
DECLARE @CMD_HISTORY NVARCHAR(1200)
DECLARE @FILE_EXISTS INT
DECLARE @CMD_EXTENDED NVARCHAR(3200)

IF @OVERRIDE_Server_Settings IS NULL SET @OVERRIDE_Server_Settings = 0
IF @USE_EXTENDED IS NULL SET @USE_EXTENDED = 1
IF @SHOW_MESSAGES_FLAG IS NULL SET @SHOW_MESSAGES_FLAG = 0
IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Starting remove expired backup operation...')

IF @OVERRIDE_Server_Settings = 1
BEGIN
 IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Override server settings...')
 IF @REMOVE_EXPIRED_PHYSICAL IS NULL SET @REMOVE_EXPIRED_PHYSICAL = (SELECT TOP 1 ISNULL([backup_expired_delete_physical], 0) FROM EZManagePro..ET_Server_Settings)
 IF @REMOVE_EXPIRED_HISTORY IS NULL SET @REMOVE_EXPIRED_HISTORY = (SELECT TOP 1 ISNULL([backup_expired_delete_history], 0) FROM EZManagePro..ET_Server_Settings)
END
 ELSE
BEGIN
 IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Use server settings...')
 SET @REMOVE_EXPIRED_PHYSICAL = (SELECT TOP 1 ISNULL([backup_expired_delete_physical], 0) FROM EZManagePro..ET_Server_Settings)
 SET @REMOVE_EXPIRED_HISTORY = (SELECT TOP 1 ISNULL([backup_expired_delete_history], 0) FROM EZManagePro..ET_Server_Settings)
END

IF @REMOVE_EXPIRED_PHYSICAL IS NULL SET @REMOVE_EXPIRED_PHYSICAL = 0
IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Remove expired backups physical files: '+CAST(@REMOVE_EXPIRED_PHYSICAL AS NVARCHAR(20)))

IF @REMOVE_EXPIRED_HISTORY IS NULL SET @REMOVE_EXPIRED_HISTORY = 0
IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Remove expired backups history: '+CAST(@REMOVE_EXPIRED_HISTORY AS NVARCHAR(20)))

CREATE TABLE #tblConfigure (
 [name] nvarchar(70),
 [minimum] int,
 [maximum] int,
 [config_value] int,
 [run_value] int
)

SET @CMDSHELLFLAG = 0

IF @REMOVE_EXPIRED_PHYSICAL != 0 OR @REMOVE_EXPIRED_HISTORY !=0
BEGIN
 IF @REMOVE_EXPIRED_PHYSICAL = 1
 BEGIN
  IF @USE_EXTENDED = 0
  BEGIN
   INSERT INTO #tblConfigure EXEC master..sp_configure
    IF EXISTS (SELECT * FROM #tblConfigure WHERE [name] = N'xp_cmdshell')
   BEGIN
   IF (SELECT [config_value] FROM #tblConfigure WHERE [name] = N'xp_cmdshell') = 0
    BEGIN
     IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Enabling command shell execution...')
     EXEC master..sp_configure N'xp_cmdshell', 1
     RECONFIGURE WITH OVERRIDE
     SET @CMDSHELLFLAG = 1
    END
   END
  END
 END

 IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Reading expired backup list...')
 IF @SHOW_MESSAGES_FLAG = 1 SELECT bkset.backup_set_id, bkfam.media_set_id, bkfam.physical_device_name FROM msdb..backupset bkset INNER JOIN msdb..backupmediafamily bkfam ON bkset.media_set_id = bkfam.media_set_id WHERE bkfam.device_type = 2 AND bkset.expiration_date < GETDATE()

 DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
 FOR
	  SELECT 
	   bkset.backup_set_id,
	   bkfam.media_set_id,
	   bkfam.physical_device_name
	  FROM 
	   msdb..backupset bkset
	  INNER JOIN
	   msdb..backupmediafamily bkfam
		ON bkset.media_set_id = bkfam.media_set_id
	  WHERE
	   bkfam.device_type = 2 AND
	   bkset.expiration_date between dateadd(month,-3,GETDATE()) and getdate()
 OPEN CUR
 FETCH NEXT FROM CUR INTO @BACKUP_SET_ID, @MEDIA_SET_ID, @PHYSICAL_DEVICE_NAME
 WHILE @@FETCH_STATUS = 0
 BEGIN
  IF @REMOVE_EXPIRED_PHYSICAL = 1
  BEGIN
   IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Checking if physical file exists "'+@PHYSICAL_DEVICE_NAME+N'"...')
   EXEC master..xp_fileexist @PHYSICAL_DEVICE_NAME, @FILE_EXISTS OUT
   IF (@FILE_EXISTS) != 0
   BEGIN
    IF @USE_EXTENDED = 1
    BEGIN
     SET @CMD_EXTENDED = N'EXEC master..DeleteFile N'''+@PHYSICAL_DEVICE_NAME+N''''
     IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Delete physical file using EZManage procedures "'+@PHYSICAL_DEVICE_NAME+N'"...')
     EXEC master..sp_executesql @CMD_EXTENDED

     --SET @DIRECTORY = REVERSE(SUBSTRING(REVERSE(RTRIM(LTRIM(@PHYSICAL_DEVICE_NAME))), CHARINDEX(N'\', REVERSE(RTRIM(LTRIM(@PHYSICAL_DEVICE_NAME)))) + 1, LEN(RTRIM(LTRIM(@PHYSICAL_DEVICE_NAME))) - CHARINDEX(N'\', REVERSE(RTRIM(LTRIM(@PHYSICAL_DEVICE_NAME)))) + 1))
     --SET @CMD_EXTENDED = N'EXEC master..xp_sql_fileoperation 2, N'''+@DIRECTORY+N''''
     --IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Checking if directory is empty "'+@DIRECTORY+N'"...')
     --EXEC master..sp_executesql @CMD_EXTENDED
    END
     ELSE
    BEGIN
     SET @CMD_PHYSICAL = N'DEL "'+@PHYSICAL_DEVICE_NAME+N'"'
     IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Deleting physical file using command shell "'+@PHYSICAL_DEVICE_NAME+N'"...')
     EXEC master..xp_cmdshell @CMD_PHYSICAL, NO_OUTPUT
    END
   END
    ELSE
   BEGIN
    IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Physical file does NOT exist...') 
   END
  END

 FETCH NEXT FROM CUR INTO @BACKUP_SET_ID, @MEDIA_SET_ID, @PHYSICAL_DEVICE_NAME
 END

 CLOSE CUR
 DEALLOCATE CUR

  IF @REMOVE_EXPIRED_HISTORY = 1
  BEGIN
  if (object_id('tempdb..#t1') is not null) drop table tempdb..#t1

   SELECT 
	   bkset.backup_set_id,
	   bkfam.media_set_id
	into #t1
	  FROM 
	   msdb..backupset bkset
	  INNER JOIN
	   msdb..backupmediafamily bkfam
		ON bkset.media_set_id = bkfam.media_set_id
	  WHERE
	   bkfam.device_type = 2 AND
	   bkset.expiration_date < GETDATE()

    delete msdb..restorefilegroup where restore_history_id in  (select restore_history_id from msdb..restorehistory inner join #t1 on restorehistory.backup_set_id = #t1.backup_set_id)
    delete msdb..restorefile where restore_history_id in  (select restore_history_id from msdb..restorehistory inner join #t1 on restorehistory.backup_set_id = #t1.backup_set_id)
	delete msdb..restorehistory where backup_set_id in (select backup_set_id from #t1) 
    
    IF  LEFT(CONVERT(VARCHAR,SERVERPROPERTY(N'ProductVersion')),-1+CHARINDEX('.',CONVERT(VARCHAR,SERVERPROPERTY(N'ProductVersion')),1)) IN(9,10)
    BEGIN
		delete msdb..backupfilegroup where backup_set_id in (select backup_set_id from #t1) 
    END

    delete msdb..backupfile where backup_set_id in (select backup_set_id from #t1) 
    delete msdb..backupfilegroup where backup_set_id  in (select backup_set_id from #t1) 
	delete msdb..backupset where media_set_id in (select media_set_id from #t1) 
    delete msdb..backupmediafamily where media_set_id in (select media_set_id from #t1) 
    delete msdb..backupmediaset where media_set_id in (select media_set_id from #t1) 
  END

END
 ELSE
BEGIN
 PRINT(N'Expiration settings are not set to remove physical or history of the backup.')
END

IF @CMDSHELLFLAG = 1
BEGIN
 IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Re-disabling command shell execution...')
 EXEC master..sp_configure N'xp_cmdshell', 0
 RECONFIGURE
END

DROP TABLE #tblConfigure
IF @SHOW_MESSAGES_FLAG = 1 PRINT (N' - Remove expired backup execution COMPLETED...')


if (@@version like '%2008%' or @@version like '%2012%' or @@version like '%2014%'or @@version like '%2016%'or @@version like '%2017%' or @@version like '%2018%' or @@version like '%2019%') 
begin	                                
	declare @toKeepFrom datetime
	set @toKeepFrom  = dateadd(month,-4,getdate())
	EXEC msdb..sp_delete_backuphistory @toKeepFrom 
end

if exists (select 1 from sys.sql_modules a where object_name(object_id) = 'SP_BACKUP_REMOVE_UNKNOWN_FILES')
exec [SP_BACKUP_REMOVE_UNKNOWN_FILES]