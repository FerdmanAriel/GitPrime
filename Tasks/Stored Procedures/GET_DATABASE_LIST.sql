--EZMANAGE_
create procedure [Tasks].[GET_DATABASE_LIST]
as

	SELECT DB_NAME(database_id) AS DatabaseName,
		   sum((size * 8) / 1024) SizeMB
	FROM master.sys.master_files
	group by DB_NAME(database_id)
	order by 2 desc