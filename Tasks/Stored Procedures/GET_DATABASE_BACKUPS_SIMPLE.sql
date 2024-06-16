--EZMANAGE_
create procedure [Tasks].[GET_DATABASE_BACKUPS_SIMPLE](@database_name nvarchar(200),@start datetime,@end datetime )
as

begin

if (@start is null)
	set @start = dateadd(day,-30,getdate())

if (@end is null)
	set @end = getdate()
	declare @sql nvarchar(max) 

        set @sql = N'
  select database_name, backup_finish_date,type,backup_size,compressed_backup_size
    from  msdb.dbo.backupset b 
    where 
	database_name = @database_name
	and backup_finish_date between @start and @end
	and [type] in (''D'',''I'',''P'',''Q'')'

	 declare @version nvarchar(500)
        set @version  = @@Version
        if (@@Version like '%2005%')
	        set @sql = replace(@sql,N'b.compressed_backup_size','b.backup_size')

        exec sp_executesql @sql		
end