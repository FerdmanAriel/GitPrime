--EZMANAGE_
CREATE PROCEDURE [dbo].[SP_BACKUP_NATIVE]
-- Old name: NATIVE_SQL_BACKUP
 @DATABASE_NAME NVARCHAR(128),
 @BACKUP_TYPE NVARCHAR(20),
 @LOCATION NVARCHAR(1024),
 @COMPRESS BIT,
 @TTL INT = NULL,
 @FTP_LOCATION NVARCHAR(1024) = NULL,
 @COPY_LOCATION NVARCHAR(1024) = NULL,
 @SHOW_PROGRESS BIT = 0, 
 @USR_BLOCKSIZE INT = NULL,
 @USR_BUFFERCOUNT INT = NULL,
 @USR_MAXTRANSFERSIZE INT = NULL,
 @BACKUP_DESCRIPTION NVARCHAR(255) = NULL,
 @INCLUDE_TIMESTAMP_IN_FILENAME BIT = 1,
 @RETURN_RESULTS BIT = 0,
 @COPY_ONLY BIT = 0,
 @WAIT_FOR_RUNNING_BACKUP_TO_FINISH BIT = 0,
 @FINAL_FILENAME NVARCHAR(1024)
AS

BEGIN
DECLARE @CMD NVARCHAR(3200)
 SET @FINAL_FILENAME =REPLACE(@FINAL_FILENAME, '.sqm', '.bak') 
		 --USE NATIVE START
		  SELECT @CMD = CASE @BACKUP_TYPE 
		   WHEN N'D' THEN N'BACKUP DATABASE ['+@DATABASE_NAME+'] TO DISK = N'''+@FINAL_FILENAME+''''
		   WHEN N'L' THEN N'BACKUP LOG ['+@DATABASE_NAME+'] TO DISK = N'''+@FINAL_FILENAME+''''
		   WHEN N'I' THEN N'BACKUP DATABASE ['+@DATABASE_NAME+'] TO DISK = N'''+@FINAL_FILENAME+''' WITH DIFFERENTIAL'
		  END


		  IF (@USR_MAXTRANSFERSIZE IS NOT NULL) AND (@USR_MAXTRANSFERSIZE > 0)
		  BEGIN
		   IF CHARINDEX(N'WITH', @CMD) > 0
			SET @CMD = @CMD+N', MAXTRANSFERSIZE = '+CAST(@USR_MAXTRANSFERSIZE AS NVARCHAR(80))
		   ELSE
			SET @CMD = @CMD+N' WITH MAXTRANSFERSIZE = '+CAST(@USR_MAXTRANSFERSIZE AS NVARCHAR(80))
		  END

		  IF (@USR_BUFFERCOUNT IS NOT NULL) AND (@USR_BUFFERCOUNT > 0)
		  BEGIN
		   IF CHARINDEX(N'WITH', @CMD) > 0
			SET @CMD = @CMD+N', BUFFERCOUNT = '+CAST(@USR_BUFFERCOUNT AS NVARCHAR(80))
		   ELSE
			SET @CMD = @CMD+N' WITH BUFFERCOUNT = '+CAST(@USR_BUFFERCOUNT AS NVARCHAR(80))
		  END

		  IF (@USR_BLOCKSIZE IS NOT NULL) AND (@USR_BLOCKSIZE > 0)
		  BEGIN
		   IF CHARINDEX(N'WITH', @CMD) > 0
			SET @CMD = @CMD+N', BLOCKSIZE = '+CAST(@USR_BLOCKSIZE AS NVARCHAR(80))
		   ELSE
			SET @CMD = @CMD+N' WITH BLOCKSIZE = '+CAST(@USR_BLOCKSIZE AS NVARCHAR(80))
		  END

		  IF (@TTL IS NOT NULL) AND (@TTL > 0) 
		  BEGIN
		   IF CHARINDEX(N'WITH', @CMD) > 0
			SET @CMD = @CMD+N', EXPIREDATE = N'''+CAST(DATEADD(DAY, @TTL, GETDATE()) AS NVARCHAR(128))+''''  
		   ELSE
			SET @CMD = @CMD+N' WITH EXPIREDATE = N'''+CAST(DATEADD(DAY, @TTL, GETDATE()) AS NVARCHAR(128))+''''  
		  END

		  IF (@BACKUP_DESCRIPTION IS NOT NULL)
		  BEGIN
		   IF CHARINDEX(N'WITH', @CMD) > 0
			SET @CMD = @CMD+N', DESCRIPTION  = N'''+@BACKUP_DESCRIPTION+''''  
		   ELSE
			SET @CMD = @CMD+N' WITH DESCRIPTION = N'''+@BACKUP_DESCRIPTION+''''  
		  END

		  IF (@SHOW_PROGRESS IS NOT NULL)
		  BEGIN
		   IF @SHOW_PROGRESS = 1
		   BEGIN
			IF CHARINDEX(N'WITH', @CMD) > 0
			 SET @CMD = @CMD+N', STATS = 1'
			ELSE
			 SET @CMD = @CMD+N' WITH STATS = 1'
		   END
		  END
		  
		  IF SUBSTRING(CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(20)), 0, CHARINDEX(N'.', CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR(20)))) > 8
		  BEGIN
		   IF @COPY_ONLY = 1
		   BEGIN
			IF CHARINDEX(N'WITH', @CMD) > 0
			 SET @CMD = @CMD+N', COPY_ONLY'
			ELSE
			 SET @CMD = @CMD+N' WITH COPY_ONLY'
		   END

		   
		   IF @COMPRESS = 1
		   BEGIN
			IF CHARINDEX(N'WITH', @CMD) > 0
			 SET @CMD = @CMD+N', COMPRESSION'
			ELSE
			 SET @CMD = @CMD+N' WITH COMPRESSION'
		   END
		  END   

		if (@@version not like '%2000%')
		begin
			declare @configurationStatus int
			-- To allow advanced options to be changed.
			begin try
				SELECT @configurationStatus = CONVERT(INT, ISNULL(value, value_in_use)) FROM  sys.configurations WHERE  name = 'show advanced options' 
				if (@configurationStatus = 0)
				begin
					EXEC sp_configure 'show advanced options', 1
					RECONFIGURE
				end
			end try
			begin catch
				EXEC sp_configure 'show advanced options', 1
				RECONFIGURE
			end catch
			
			begin try
				SELECT @configurationStatus = CONVERT(INT, ISNULL(value, value_in_use)) FROM  sys.configurations WHERE  name = 'xp_cmdshell' 
				if (@configurationStatus = 0)
				begin
					EXEC sp_configure 'xp_cmdshell', 1
					RECONFIGURE
				end
			end try
			begin catch
				EXEC sp_configure 'xp_cmdshell', 1
				RECONFIGURE
			end catch
		end
				
		-------------------------------------------------------------
		  --create directories if not exists
		EXEC (N'EXEC master..xp_cmdshell N''MD "'+@LOCATION+N'"'', NO_OUTPUT')

		  IF @SHOW_PROGRESS = 1
		   PRINT(N'Starting native backup to: "'+@FINAL_FILENAME+'"')
		  ELSE
		   PRINT(N'Starting native backup...')

		--print @CMD
		  EXEC master..sp_executesql @CMD
		  --USE NATIVE END
	
END