--EZMANAGE_

create procedure [dbo].[SP_BACKUP_GET_ESTIMATED_BACKUP_SIZE] as
-- Old name: GetEstimatedCloudBackupSizes

declare @result int 
set @result = 0

declare @server_name nvarchar(500), @database_name nvarchar(500)
declare db_cursor CURSOR FOR  
select distinct server_name,database_name 
from 
	ET_RULES rules inner join 
	ET_ACTION act on rules.id = act.id inner join 
	ET_RULE_ATTACH b on rules.rule_name = b.rule_name 
where 
	b.rule_name in ('EZ Manage SQL: FULL DATA DAILY') and 
	para_nvarchar_3 <> ''

OPEN db_cursor   
FETCH NEXT FROM db_cursor INTO @server_name, @database_name

WHILE @@FETCH_STATUS = 0   
BEGIN   
	   declare @maxSnapshotID int  
		select top 1 @maxSnapshotID = ID from Database_Files_Snapshots
		where Server_Name = @server_name
		order by Sample_Time desc
	
		if (@maxSnapshotID Is Not null)
		begin
			select @result=@result+isnull(sum(Size_On_Disk) ,0)
			from Database_Files_Statistics	
			where Database_Files_Snapshot_ID = @maxSnapshotID and Database_Name = @database_name
			
		end
       FETCH NEXT FROM db_cursor INTO @server_name, @database_name
END   

CLOSE db_cursor   
DEALLOCATE db_cursor
select @result/1000
-- For each database we bring in the latest statistics we have on it.