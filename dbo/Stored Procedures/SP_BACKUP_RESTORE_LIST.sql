--EZMANAGE_
CREATE PROCEDURE [dbo].[SP_BACKUP_RESTORE_LIST]
-- Old name: EP_RESTORE_LIST
    @DB_NAME NVARCHAR(128) ,
    @SELECTED_BACKUP_SET_ID INT
   -- WITH ENCRYPTION
AS 
    SET NOCOUNT ON 
    DECLARE @TYPE NCHAR(1)
    DECLARE @PHYSICAL_DEVICE_NAME NVARCHAR(600)

	if (object_id('tempdb..#resultData') is not null) drop table tempdb..#resultData;
	if (object_id('tempdb..#resultDiff') is not null) drop table tempdb..#resultDiff;
	if (object_id('tempdb..#resultLog') is not null) drop table tempdb..#resultLog;

	-- 3 Steps - 
	-- 1. Find the data file we are starting from
	-- 2. Find all DIFF files between the data file from #1 and the closest time we have
	-- 3. From the LOG files find the ones bigger than the last DIFF file

	-- Step 1 - the data file. 
	 SELECT  top 1 bkset.backup_set_id ,                bkset.media_set_id ,                bkset.position ,                bkset.first_lsn ,                bkset.last_lsn ,                bkset.backup_finish_date ,                bkset.type ,                bkfam.physical_device_name
	 into #resultData
        FROM    msdb..backupset bkset
                INNER JOIN msdb..backupmediafamily bkfam ON bkset.media_set_id = bkfam.media_set_id
        WHERE   bkfam.family_sequence_number = 1
                AND bkset.database_name = @DB_NAME
			--	and dbo.fn_FileExists(bkfam.physical_device_name) = 1
                AND bkset.backup_set_id <= @SELECTED_BACKUP_SET_ID and bkset.type = 'D'
        ORDER BY bkset.backup_finish_date DESC

		delete #resultData where dbo.fn_FileExists(physical_device_name) = 0
--select * from #resultData -- DEBUG

--If we don't have a full data we have nothing to return
if ((select count(*) from #resultData) = 0)
begin
	select * from #resultData
	return
end

declare @full_data_backup_set_id int
select @full_data_backup_set_id = backup_set_id from #resultData

	-- Step 2 - the diff files (if exist)
	 SELECT  bkset.backup_set_id ,                bkset.media_set_id ,                bkset.position ,                bkset.first_lsn ,                bkset.last_lsn ,                bkset.backup_finish_date ,                bkset.type ,                bkfam.physical_device_name
	 into #resultDiff
        FROM    msdb..backupset bkset
                INNER JOIN msdb..backupmediafamily bkfam ON bkset.media_set_id = bkfam.media_set_id
        WHERE   bkfam.family_sequence_number = 1
                AND bkset.database_name = @DB_NAME
                AND bkset.backup_set_id between  @full_data_backup_set_id and @SELECTED_BACKUP_SET_ID and bkset.type = 'I' 
				and EZManagePro.dbo.fn_FileExists(bkfam.physical_device_name) = 1

        ORDER BY bkset.backup_finish_date DESC

--select * from #resultDiff

	declare @max_dataBackup_diff_set_id bigint
	select top 1 @max_dataBackup_diff_set_id = max(backup_set_id) from #resultDiff

	if (@max_dataBackup_diff_set_id is null)
		set @max_dataBackup_diff_set_id = @full_data_backup_set_id

	-- Step 3 - the log files (if exist)
	 SELECT  bkset.backup_set_id ,                bkset.media_set_id ,                bkset.position ,                bkset.first_lsn ,                bkset.last_lsn ,                bkset.backup_finish_date ,                bkset.type ,                bkfam.physical_device_name
	 into #resultLog
        FROM    msdb..backupset bkset
                INNER JOIN msdb..backupmediafamily bkfam ON bkset.media_set_id = bkfam.media_set_id
        WHERE   bkfam.family_sequence_number = 1
                AND bkset.database_name = @DB_NAME
                AND bkset.backup_set_id between  @max_dataBackup_diff_set_id and @SELECTED_BACKUP_SET_ID and bkset.type = 'L' 
        ORDER BY bkset.backup_finish_date DESC

select * from #resultData
union all 
select * from #resultDiff
union all
select * from #resultLog
order by backup_finish_date



--CREATE PROCEDURE SP_BACKUP_RESTORE_LIST
-- @DB_NAME NVARCHAR(128),
-- @SELECTED_BACKUP_SET_ID INT
--WITH ENCRYPTION
--AS

--CREATE TABLE #tblBACKUP (
-- backup_set_id int,
-- media_set_id int,
-- position int,
-- first_lsn numeric(25, 0),
-- last_lsn numeric(25, 0),
-- backup_finish_date datetime,
-- type nchar(1),
-- physical_device_name nvarchar(600)
--)

--DECLARE @RESTORE_TYPE NCHAR(1)
--SELECT TOP 1 @RESTORE_TYPE = [type] FROM msdb..backupset WHERE backup_set_id = @SELECTED_BACKUP_SET_ID

--DECLARE @FINISHED BIT
--SET @FINISHED = 0

--DECLARE @DIFF_ADDED BIT
--SET @DIFF_ADDED = 0

--DECLARE @BACKUP_SET_ID INT
--DECLARE @MEDIA_SET_ID INT
--DECLARE @POSITION INT
--DECLARE @FIRST_LSN NUMERIC(25, 0)
--DECLARE @LAST_LSN NUMERIC(25, 0)
--DECLARE @BACKUP_FINISH_DATE DATETIME
--DECLARE @TYPE NCHAR(1)
--DECLARE @PHYSICAL_DEVICE_NAME NVARCHAR(600)

--DECLARE CUR CURSOR LOCAL FORWARD_ONLY READ_ONLY
--FOR

--SELECT
--bkset.backup_set_id,
--bkset.media_set_id,
--bkset.position,
--bkset.first_lsn,
--bkset.last_lsn,
--bkset.backup_finish_date,
--bkset.type,
--bkfam.physical_device_name
--FROM
--msdb..backupset bkset
--INNER JOIN
--msdb..backupmediafamily bkfam
--	ON bkset.media_set_id = bkfam.media_set_id
--WHERE
--bkfam.family_sequence_number = 1 AND
--bkset.database_name = @DB_NAME AND
--bkset.backup_set_id <= @SELECTED_BACKUP_SET_ID
--ORDER BY
--bkset.backup_finish_date DESC

--OPEN CUR
--FETCH NEXT FROM CUR INTO @BACKUP_SET_ID, @MEDIA_SET_ID, @POSITION, @FIRST_LSN, @LAST_LSN, @BACKUP_FINISH_DATE, @TYPE, @PHYSICAL_DEVICE_NAME
--WHILE @@FETCH_STATUS = 0
--BEGIN

--IF @FINISHED = 0
--BEGIN

--IF @RESTORE_TYPE = N'I'
--BEGIN
-- IF @TYPE = N'I'
-- BEGIN
--  IF @DIFF_ADDED = 0
--  BEGIN
--   INSERT INTO #tblBACKUP (backup_set_id, media_set_id, position, first_lsn, last_lsn, backup_finish_date, type, physical_device_name)
--   VALUES (@BACKUP_SET_ID, @MEDIA_SET_ID, @POSITION, @FIRST_LSN, @LAST_LSN, @BACKUP_FINISH_DATE, @TYPE, @PHYSICAL_DEVICE_NAME)
--  END
--  SET @DIFF_ADDED = 1
-- END
--  ELSE
-- BEGIN
--  IF @TYPE = N'D'
--  BEGIN
--   INSERT INTO #tblBACKUP (backup_set_id, media_set_id, position, first_lsn, last_lsn, backup_finish_date, type, physical_device_name)
--   VALUES (@BACKUP_SET_ID, @MEDIA_SET_ID, @POSITION, @FIRST_LSN, @LAST_LSN, @BACKUP_FINISH_DATE, @TYPE, @PHYSICAL_DEVICE_NAME)
--  END
-- END
--END

--IF @RESTORE_TYPE = N'L'
--BEGIN
-- INSERT INTO #tblBACKUP (backup_set_id, media_set_id, position, first_lsn, last_lsn, backup_finish_date, type, physical_device_name)
-- VALUES (@BACKUP_SET_ID, @MEDIA_SET_ID, @POSITION, @FIRST_LSN, @LAST_LSN, @BACKUP_FINISH_DATE, @TYPE, @PHYSICAL_DEVICE_NAME)
--END

--IF @TYPE = N'D' SET @FINISHED = 1
--END

--FETCH NEXT FROM CUR INTO @BACKUP_SET_ID, @MEDIA_SET_ID, @POSITION, @FIRST_LSN, @LAST_LSN, @BACKUP_FINISH_DATE, @TYPE, @PHYSICAL_DEVICE_NAME
--END
--CLOSE CUR
--DEALLOCATE CUR

--DECLARE @DIFF_FOUND BIT
--SET @DIFF_FOUND = 0

--DECLARE CUR_FIX CURSOR LOCAL FORWARD_ONLY READ_ONLY
--FOR
-- SELECT backup_set_id, media_set_id, type, physical_device_name, backup_finish_date FROM #tblBACKUP ORDER BY backup_finish_date DESC 
--OPEN CUR_FIX
--FETCH NEXT FROM CUR_FIX INTO @BACKUP_SET_ID, @MEDIA_SET_ID, @TYPE, @PHYSICAL_DEVICE_NAME, @BACKUP_FINISH_DATE
--WHILE @@FETCH_STATUS = 0
--BEGIN

--IF @DIFF_FOUND = 1
--BEGIN
-- IF (@TYPE = N'L') DELETE #tblBACKUP WHERE backup_set_id = @BACKUP_SET_ID AND media_set_id = @MEDIA_SET_ID AND type = @TYPE
--END
-- ELSE
--BEGIN
-- IF (@TYPE = N'I') SET @DIFF_FOUND = 1
--END

--FETCH NEXT FROM CUR_FIX INTO @BACKUP_SET_ID, @MEDIA_SET_ID, @TYPE, @PHYSICAL_DEVICE_NAME, @BACKUP_FINISH_DATE
--END
--CLOSE CUR_FIX
--DEALLOCATE CUR_FIX

--SELECT * FROM #tblBACKUP ORDER BY backup_finish_date ASC 

--DROP TABLE #tblBACKUP