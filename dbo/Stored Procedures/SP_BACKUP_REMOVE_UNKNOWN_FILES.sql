--EZMANAGE_
create procedure [dbo].[SP_BACKUP_REMOVE_UNKNOWN_FILES] as
-- Old name: EZME_Remove_Unknown_Files
begin
 
if (@@version like '%2000%')
return
 
DECLARE @CMDSHELLFLAG BIT
set @CMDSHELLFLAG = 0
 
DECLARE @backup_folder_server BIT
declare @backupfolder nvarchar(400)
declare @SERVERNAME nvarchar(400)
set @backupfolder= (select default_backup_folder from EZManagePro.dbo.ET_Server_Settings)
set @backup_folder_server= (select backup_folder_server from EZManagePro.dbo.ET_Server_Settings)
select @SERVERNAME=@@SERVERNAME
 
 
CREATE TABLE #tblConfigure (
[name] nvarchar(70),
[minimum] int,
[maximum] int,
[config_value] int,
[run_value] int
)
 
-- Creating an index that will help query msdb
if ((select count(*) from msdb.sys.indexes where name = 'backupmediafamily_ix1') = 0)
create index backupmediafamily_ix1 on msdb..backupmediafamily (physical_device_name)
 
EXEC sp_configure 'show advanced options', 1
RECONFIGURE  WITH OVERRIDE
 
   INSERT INTO #tblConfigure EXEC master..sp_configure
 
   IF EXISTS (SELECT * FROM #tblConfigure WHERE [name] = N'xp_cmdshell')
   BEGIN
   IF (SELECT [config_value] FROM #tblConfigure WHERE [name] = N'xp_cmdshell') = 0
BEGIN
PRINT (N' - Enabling command shell execution...')
EXEC master..sp_configure N'xp_cmdshell', 1
RECONFIGURE WITH OVERRIDE
SET @CMDSHELLFLAG = 1
END
   END
 
DECLARE @Backup_Directory nvarchar(4000)
DECLARE @backup_directories CURSOR
SET @backup_directories = CURSOR FOR
       select distinct LEFT( bkfam.physical_device_name , LEN( bkfam.physical_device_name ) -  CHARINDEX('\', REVERSE( bkfam.physical_device_name )))
       FROM msdb..backupset bkset INNER JOIN msdb..backupmediafamily bkfam ON bkset.media_set_id = bkfam.media_set_id
       WHERE bkfam.device_type = 2 AND bkset.expiration_date < GETDATE()
OPEN @backup_directories
FETCH NEXT FROM @backup_directories INTO @Backup_Directory
WHILE @@FETCH_STATUS = 0
BEGIN
       print 'Testing directory ' + @Backup_Directory
       IF OBJECT_ID('tempdb..#DirTree') IS NOT NULL
              DROP TABLE #DirTree
 
       CREATE TABLE #DirTree (
              Backup_File_Name nvarchar(255),
              Depth smallint,
              File_Flag bit
          )
 
          INSERT INTO #DirTree (Backup_File_Name, Depth, File_Flag)
          EXEC master..xp_dirtree @Backup_Directory, 0, 1
       --   select * from  #DirTree
          --select * from #DirTree
          delete #DirTree  where File_Flag = 0 --or Backup_File_Name not like '%.%' -- don't want to remove any directories
          --delete #DirTree where reverse(left(reverse(Backup_File_Name), charindex('.', reverse(Backup_File_Name)) - 1)) <> 'sqmc'
                           --                   and reverse(left(reverse(Backup_File_Name), charindex('.', reverse(Backup_File_Name)) - 1)) <> 'bak'
                           --                   and reverse(left(reverse(Backup_File_Name), charindex('.', reverse(Backup_File_Name)) - 1)) <> 'trn'
 
              -- Looping the files, removing any file that doesn't exist in the msdb repository
              DECLARE @Backup_File_Name nvarchar(4000)
              DECLARE @allFiles CURSOR
              SET @allFiles = CURSOR FOR select Backup_File_Name FROM #DirTree
                    
              OPEN @allFiles
              FETCH NEXT FROM @allFiles INTO @Backup_File_Name
              WHILE @@FETCH_STATUS = 0
              BEGIN
             
              if (select count(*) from msdb..backupmediafamily bkfam
                     where physical_device_name like '%' + @Backup_File_Name) = 0
                     begin
                                           IF @backup_folder_server=1
                                            AND (@Backup_Directory LIKE '%'+@backupfolder+'\'+REPLACE(@SERVERNAME,'\','$')+'%')
                                                  BEGIN
                                
                                                        declare @CMD_PHYSICAL nvarchar(4000)
                                                        set @CMD_PHYSICAL = N'DEL "'+ @Backup_Directory + '\' + @Backup_File_Name+N'"'
                                                        print @CMD_PHYSICAL
                                                        EXEC master..xp_cmdshell @CMD_PHYSICAL, NO_OUTPUT
                                                  END
                                               ELSE IF @backup_folder_server=0
                                                     AND (@Backup_Directory LIKE '%'+@backupfolder+'%')
                                
                                                        BEGIN
                                                     --  declare @CMD_PHYSICAL nvarchar(4000)
                                                        set @CMD_PHYSICAL = N'DEL "'+ @Backup_Directory + '\' + @Backup_File_Name+N'"'
                                                        print @CMD_PHYSICAL
                                                        EXEC master..xp_cmdshell @CMD_PHYSICAL, NO_OUTPUT
                                                        END
                                               ELSE
                                               BEGIN
                                               PRINT 'WRONG FOLDER'
                                               END
                                                       
                                                  
                     end
                     FETCH NEXT FROM @allFiles INTO @Backup_File_Name
              END
              CLOSE @allFiles
              DEALLOCATE @allFiles
FETCH NEXT FROM @backup_directories INTO @Backup_Directory
END
 
CLOSE @backup_directories
DEALLOCATE @backup_directories
 
IF @CMDSHELLFLAG = 1
BEGIN
PRINT (N' - Re-disabling command shell execution...')
EXEC master..sp_configure N'xp_cmdshell', 0
RECONFIGURE
END
 
DROP TABLE #tblConfigure
 
end