--EZMANAGE_
create procedure [Tasks].[GET_SCHEMA_TABLES](@database_name nvarchar(200),
												@filterBySchemaName nvarchar(200) = null,
												@filterByTableName nvarchar(200) = null)
as

begin

declare @sql nvarchar(max)
set @sql= '
use ['+@database_name+']
select distinct sizes.[Table Name],sizes.[Total Size],sizes.[Row Count],c.name [File_Group_Name],
c.type_desc [File_Group_Type],
b.type_desc [Type],b.create_date [Created_Date]
from sys.tables b  inner join 
	sys.indexes i on i.object_id = b.object_id inner join
	sys.filegroups c on c.data_space_id = i.data_space_id inner join
	(
	
		select a.object_id,[Table Name]=''[''+object_schema_name(a.object_id) + ''].['' + object_name(a.object_id)+'']''
		, [Row Count]=sum(case when a.index_id < 2 then row_count else 0 end)
		, [Total Size] =8*sum(reserved_page_count)/1024.0+8*sum( case 
			 when a.index_id<2 then in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count 
			 else lob_used_page_count + row_overflow_used_page_count 
			end )/1024.0
		from 
			sys.dm_db_partition_stats a 
		where a.object_id > 1024 
			and (@filterBySchemaName is null or object_schema_name(a.object_id)  collate SQL_Latin1_General_CP1_CI_AS = @filterBySchemaName collate SQL_Latin1_General_CP1_CI_AS) --and
			and (@filterByTableName is null or object_name(a.object_id)  collate SQL_Latin1_General_CP1_CI_AS= @filterByTableName  collate SQL_Latin1_General_CP1_CI_AS)
		group by a.object_id
	) sizes on sizes.object_id = i.object_id	

'
print @sql

EXEC sp_executesql @sql, N'@filterBySchemaName nvarchar(200),@filterByTableName nvarchar(200)',
							@filterBySchemaName = @filterBySchemaName,
							@filterByTableName = @filterByTableName



end