--EZMANAGE_
create PROCEDURE [dbo].[SP_MAINTENANCE_STATISTICS]
-- Old name: EZ_MAINTENANCE_STATISTICS
    @DB_NAME NVARCHAR(1200) = N'',
    @IS_FULL BIT = 0 
	--WITH ENCRYPTION
AS 
BEGIN
    

DECLARE @SQLversion nvarchar(128)
SET @SQLversion = CAST(serverproperty('ProductVersion') AS nvarchar)
SET @SQLversion = SUBSTRING(@SQLversion, 1, CHARINDEX('.', @SQLversion) - 1)
 
IF (@SQLversion <> 8) 
BEGIN 
  IF (@IS_FULL = 0 ) 
	  BEGIN
	  SET NOCOUNT ON
	  DECLARE @cmd nvarchar(1000)
	  SET @cmd = 
		'DECLARE @sql_string varchar(1000)
		DECLARE update_stat_cursor CURSOR FOR 
     	select ''UPDATE STATISTICS '' + ''[''+SCHEMA_NAME(schema_id)+''].[''+name+'']'' + '' WITH FULLSCAN;''
		from sys.tables
		where type = ''U''
		
		

		OPEN update_stat_cursor
		FETCH NEXT FROM update_stat_cursor
		INTO @sql_string

		WHILE @@FETCH_STATUS = 0
		BEGIN
			   exec (@sql_string)

		FETCH NEXT FROM update_stat_cursor INTO @sql_string
		END
		close update_stat_cursor
		DEALLOCATE update_stat_cursor'
		EXEC (@cmd)
		
	  END
  ELSE
	  BEGIN
		DECLARE @TEMP AS NVARCHAR(1200) 
		SET @TEMP = '['+ @DB_NAME +']..sp_updatestats'
		 EXEC @TEMP
	  END
END
END 
--------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------SQL 2000-------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------

IF  (@SQLversion = 8) 
BEGIN
IF (@IS_FULL = 0 ) 
	  BEGIN
	  SET NOCOUNT ON
		DECLARE @sql_string varchar(1000)
		DECLARE update_stat_cursor CURSOR FOR 
     	select 'UPDATE STATISTICS ' + '['+ table_schema + ']' + '.' + '[' + table_name +']' + ' WITH FULLSCAN;' 
     	from  INFORMATION_SCHEMA.tables inner join sysobjects on sysobjects.name = INFORMATION_SCHEMA.tables.TABLE_NAME 
     	where xtype = 'U'
		
		

		OPEN update_stat_cursor
		FETCH NEXT FROM update_stat_cursor
		INTO @sql_string

		WHILE @@FETCH_STATUS = 0
		BEGIN
			   exec (@sql_string)

		FETCH NEXT FROM update_stat_cursor INTO @sql_string
		END
		close update_stat_cursor
		DEALLOCATE update_stat_cursor

	  END
  ELSE
	  BEGIN
		DECLARE @TEMP2000 AS NVARCHAR(1200) 
		SET @TEMP2000 = '['+ @DB_NAME +']..sp_updatestats'
		 EXEC @TEMP2000
	  END
END;