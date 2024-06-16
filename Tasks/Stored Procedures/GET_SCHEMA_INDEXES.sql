--EZMANAGE_
create procedure [Tasks].[GET_SCHEMA_INDEXES](@database_name nvarchar(200) )
as

begin

declare @sql nvarchar(max) 
set @sql = '
use ['+@database_name+']
	
  SELECT 
  ind.object_id as ID, 
  isnull(ind.name,tab.name) as Index_Name,
  SCHEMA_NAME(tab.schema_id) + ''.'' + tab.name as Table_Name, 
  case when ind.type_desc = ''CLUSTERED'' then ''Yes'' else ''No'' end [Clustered],
	ind.fill_factor [Fill_Factor],
	ind.type_desc [Index Type], 
  0 as [AVG_Fragmentation_Size_In_Pages],
  0 as  [AVG_Fragmentation_IN_Percent],
  (storage_size.Pages) Pages,
 (storage_size.IndexSizeMB) [Index_Size_MB]
 -- Scan density, Extent Fragmentation
 -- those dont mean nothing - please read here. https://social.msdn.microsoft.com/Forums/sqlserver/en-US/c5cd7b9f-7085-4a71-996c-e35e5738c118/scan-density-vs-fragmentation?forum=sqldatabaseengine
 -- everything you need is here.
FROM 
sys.tables tab 
inner join sys.indexes ind on (ind.object_id = tab.object_id)
inner join (
			SELECT s.object_id,s.index_id 
				,SUM(s.[used_page_count]) * 8192 / 1024 / 1024 AS IndexSizeMB,
				SUM(s.[used_page_count]) Pages
			FROM sys.dm_db_partition_stats AS s
			GROUP BY s.object_id,s.index_id
		) storage_size on storage_size.object_id = ind.object_id and storage_size.index_id = ind.index_id
		where storage_size.Pages > 100
and storage_size.IndexSizeMB  > 5
and isnull(ind.name,tab.name) != tab.name 
--inner join
--(
--	select object_id, 
--	avg(avg_fragment_size_in_pages) [AVG_Fragmentation_Size_In_Pages],
--  avg(avg_fragmentation_in_percent) [AVG_Fragmentation_IN_Percent],
--  max(index_depth) [Index Depth]
--  from  sys.dm_db_index_physical_stats (null, null, NULL, NULL , ''limited'')
--  where alloc_unit_type_desc not in (''ROW_OVERFLOW_DATA'',''LOB_DATA'')   
--  group by object_id
--) phy on phy.object_id = ind.object_id'

EXEC sp_EXECUTESQL @sql



end