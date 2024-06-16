--EZMANAGE_
create procedure [dbo].[SP_BACKUP_GET_LSN_FROM_FILE]( @BackupFile as varchar(1000), @LSN as varchar(256) output) as 
-- Old name: spGetLSNFromBACKUPFile
--declare @BackupFile as varchar(1000) = N'C:\Program Files\Microsoft SQL Server\MSSQL13.SQL2016EXPRESS\MSSQL\Backup\a1\LOG\rayman-pc$SQL2016EXPRESS_a1_20181011_0657_00_740_LOG.trn'
--declare @LSN as varchar(256)
declare @BackupDT datetime
declare @sql varchar(8000)
declare @ProductVersion NVARCHAR(128)
declare @ProductVersionNumber TINYINT

SET @ProductVersion = CONVERT(NVARCHAR(128),SERVERPROPERTY('ProductVersion'))
SET @ProductVersionNumber = SUBSTRING(@ProductVersion, 1, (CHARINDEX('.', @ProductVersion) - 1))

set @sql = 'create table dbo.tblBackupHeader
( 
    BackupName varchar(256),    BackupDescription varchar(256),    BackupType varchar(256),            ExpirationDate varchar(256),    Compressed varchar(256),    Position varchar(256),    DeviceType varchar(256),            UserName varchar(256),    ServerName varchar(256),    DatabaseName varchar(256),    DatabaseVersion varchar(256),            DatabaseCreationDate varchar(256),    BackupSize varchar(256),    FirstLSN varchar(256),    LastLSN varchar(256),            CheckpointLSN varchar(256),    DatabaseBackupLSN varchar(256),    BackupStartDate varchar(256),    BackupFinishDate varchar(256),            SortOrder varchar(256),    CodePage varchar(256),    UnicodeLocaleId varchar(256),    UnicodeComparisonStyle varchar(256),            CompatibilityLevel varchar(256),    SoftwareVendorId varchar(256),    SoftwareVersionMajor varchar(256),        
    SoftwareVersionMinor varchar(256),    SoftwareVersionBuild varchar(256),    MachineName varchar(256),    Flags varchar(256),            BindingID varchar(256),    RecoveryForkID varchar(256),    Collation varchar(256),    FamilyGUID varchar(256),            HasBulkLoggedData varchar(256),    IsSnapshot varchar(256),    IsReadOnly varchar(256),    IsSingleUser varchar(256),            HasBackupChecksums varchar(256),    IsDamaged varchar(256),    BeginsLogChain varchar(256),    HasIncompleteMetaData varchar(256),            IsForceOffline varchar(256),    IsCopyOnly varchar(256),    FirstRecoveryForkID varchar(256),    ForkPointLSN varchar(256),            RecoveryModel varchar(256),    DifferentialBaseLSN varchar(256),    DifferentialBaseGUID varchar(256),            BackupTypeDescription varchar(256),    BackupSetGUID varchar(256),    CompressedBackupSize varchar(256),'

-- THIS IS GENERIC FOR SQL SERVER 2008R2, 2012 and 2014

-- THIS IS SPECIFIC TO SQL SERVER 2012
if @ProductVersionNumber in(11)
set @sql = @sql +'
    Containment varchar(256),'

-- THIS IS SPECIFIC TO SQL SERVER 2014
--if @ProductVersionNumber in(12)
--set @sql = @sql +'
--    Containment tinyint, 
--    KeyAlgorithm nvarchar(32), 
--    EncryptorThumbprint varbinary(20), 
--    EncryptorType nvarchar(32),'

if @ProductVersionNumber >= 12 -- Should take care of all future versions as well.
set @sql = @sql +'
    Containment tinyint, 
    KeyAlgorithm nvarchar(32), 
    EncryptorThumbprint varbinary(20), 
    EncryptorType nvarchar(32),'



--All versions (This field added to retain order by)
set @sql = @sql +'
    Seq int NOT NULL identity(1,1)
); 
'
--create the temporary table to hold the values
exec (@sql)

declare @notNative bit 
set @notNative = 0
begin try
	set @sql = 'restore headeronly from disk = '''+ @BackupFile +'''' 
	insert into dbo.tblBackupHeader exec(@sql)
end try
begin catch
	print 'not native'
	set @notNative = 1
end catch

if (@notNative = 1)
begin
	declare @VDIID  NVARCHAR(256)
	SET @VDIID = CAST(NEWID() AS NVARCHAR(80))
	exec master..xp_sql_vbdcreate @@SERVICENAME, @BackupFile, 1, @VDIID, 1, '',NULL,NULL,NULL,NULL,60000

	set @sql = 'restore headeronly from VIRTUAL_DEVICE = '''+ @VDIID +'''' 

	print @sql

	insert into dbo.tblBackupHeader 
	exec(@sql)
end 

select top 1 @LSN =  LastLSN from dbo.tblBackupHeader 

if object_id('dbo.tblBackupHeader') is not null drop table dbo.tblBackupHeader

print @LSN