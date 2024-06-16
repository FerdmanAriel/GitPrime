--EZMANAGE_
CREATE PROCEDURE [dbo].[SP_INTEGRITY]  
-- old name: EP_INTEGRITY
@DB_NAME       NVARCHAR(128), 
        @REPAIR_TYPE   NVARCHAR(128), 
        @WITH_INDEX    BIT = 1, 
        @NO_INFO_MSG   BIT = 1, 
        @IS_JOB_ACTION BIT = 0  as

--SET @DB_NAME = N'DemoDataPurity' 
--SET @REPAIR_TYPE = N'REPAIR_REBUILD' 
--SET @WITH_INDEX = 1 
--SET @NO_INFO_MSG = 0 
--SET @IS_JOB_ACTION = 1 

if (object_id('tempdb..#tblcheck') is not null) DROP TABLE #tblcheck

DECLARE @WITH_INDEX_TEXT NVARCHAR(80), 
        @VERSION         INT 

IF @WITH_INDEX = 1 
  SET @WITH_INDEX_TEXT = N'' 
ELSE 
  SET @WITH_INDEX_TEXT = N', NOINDEX' 

-----------------#tblCheck2000 
SELECT @VERSION = LEFT(CONVERT(VARCHAR, Serverproperty(N'ProductVersion')), -1 + 
                                    Charindex('.', CONVERT(VARCHAR, 
                                                   Serverproperty( 
                                                   N'ProductVersion')), 
                                    1)) 

IF @VERSION = 8 --SQL 2000 
  BEGIN 
      CREATE TABLE #tblcheck2000 
        ( 
           [error]       INT, 
           [level]       INT, 
           [state]       INT, 
           [messagetext] NVARCHAR(256), 
           [repairlevel] NVARCHAR(700), 
           [status]      INT, 
           [dbid]        INT, 
           [indid]       INT, 
           [id]          INT, 
           [file]        INT, 
           [page]        INT, 
           [slot]        INT, 
           [reffile]     INT, 
           [refpage]     INT, 
           [refslot]     INT, 
           [allocation]  INT 
        ) 
  END 
ELSE IF @VERSION > 8 
   AND @VERSION < 12 --SQL 2005, 2008, 2008R2, 2012 
  BEGIN 
      CREATE TABLE #tblcheck2005to2008r2 
        ( 
           [error]       [BIGINT] NULL, 
           [level]       [BIGINT] NULL, 
           [state]       [BIGINT] NULL, 
           [messagetext] [VARCHAR](7000) NULL, 
           [repairlevel] [VARCHAR](256) NULL, 
           [status]      [BIGINT] NULL, 
           [dbid]        [BIGINT] NULL, 
           [id]          [BIGINT] NULL, 
           [indid]       [BIGINT] NULL, 
           [partitionid] [BIGINT] NULL, 
           [allocunitid] [BIGINT] NULL, 
           [file]        [BIGINT] NULL, 
           [page]        [BIGINT] NULL, 
           [slot]        [BIGINT] NULL, 
           [reffile]     [BIGINT] NULL, 
           [refpage]     [BIGINT] NULL, 
           [refslot]     [BIGINT] NULL, 
           [allocation]  [BIGINT] NULL 
        ) 
  END 
ELSE -- SQL 2014 and above 
  BEGIN 
      CREATE TABLE #tblcheck 
        ( 
           [error]       [INT] NULL, 
           [level]       [INT] NULL, 
           [state]       [INT] NULL, 
           [messagetext] [VARCHAR](7000) NULL, 
           [repairlevel] [NVARCHAR](max) NULL, 
           [status]      [INT] NULL, 
           [dbid]        [INT] NULL, 
           [dbfragid]    [INT] NULL, 
           [objectid]    [INT] NULL, 
           [indexid]     [INT] NULL, 
           [partitionid] [BIGINT] NULL, 
           [allocunitid] [BIGINT] NULL, 
           [riddbid]     [INT] NULL, 
           [ridpruid]    [INT] NULL, 
           [file]        [INT] NULL, 
           [page]        [INT] NULL, 
           [slot]        [INT] NULL, 
           [refdbid]     [INT] NULL, 
           [refpruid]    [INT] NULL, 
           [reffile]     [INT] NULL, 
           [refpage]     [INT] NULL, 
           [refslot]     [INT] NULL, 
           [allocation]  [INT] NULL 
        ) 
  END 

DECLARE @CMD AS NVARCHAR(3200) 

-- This checks if we even need to attempt a recovery
SET @CMD = N'DBCC CHECKDB (N''' + @DB_NAME + '''' + @WITH_INDEX_TEXT 
           + ') WITH TABLERESULTS, NO_INFOMSGS' 

IF @VERSION = 8 
  BEGIN 
      INSERT INTO #tblcheck2000 
      EXEC master..sp_executesql @CMD 

      SELECT *  FROM   #tblcheck2000 

      IF EXISTS (SELECT [id] 
                 FROM   #tblcheck2000) 
        BEGIN 
                  SET @CMD = 
                  N'ALTER DATABASE [' + @DB_NAME + '] SET OFFLINE WITH ROLLBACK IMMEDIATE 
				  ALTER DATABASE [' + @DB_NAME + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE 
				  ALTER DATABASE [' + @DB_NAME + '] SET ONLINE WITH ROLLBACK IMMEDIATE 
				  DBCC CHECKDB (N''' + @DB_NAME + ''', ' + @REPAIR_TYPE + ') WITH NO_INFOMSGS
				  ALTER DATABASE [' + @DB_NAME + '] SET MULTI_USER WITH ROLLBACK IMMEDIATE' 

                  EXEC master..sp_executesql 
                    @CMD 
        END 

      IF ( @IS_JOB_ACTION = 1 ) 
        BEGIN 
            DECLARE @MSGTEXT NVARCHAR(1200) 
            DECLARE cursoralerts CURSOR FOR 
              SELECT [messagetext] 
              FROM   #tblcheck2000 

            OPEN cursoralerts 

            FETCH next FROM cursoralerts INTO @MSGTEXT 

            WHILE @@FETCH_STATUS = 0 
              BEGIN 
                  IF NOT EXISTS(SELECT * 
                                FROM   [EZManagePro].[dbo].[et_alerts] 
                                WHERE  [alert_desc] = @MSGTEXT) 
                    INSERT INTO [EZManagePro].[dbo].[et_alerts] 
                    VALUES      ('Integrity check job alert', 
                                 '1', 
                                 Getdate(), 
                                 NULL, 
                                 1, 
                                 @DB_NAME, 
                                 'Database Files', 
                                 @MSGTEXT, 
                                 0, 
                                 0, 
                                 '', 
                                 0, 
                                 0, 
                                 3, 
                                 NULL, 
                                 NULL, 
                                 '1') 

                  FETCH next FROM cursoralerts INTO @MSGTEXT 
              END 

            CLOSE cursoralerts 

            DEALLOCATE cursoralerts 
        END 
  END 
ELSE IF @VERSION > 8 
   AND @VERSION < 12 
  BEGIN 
      INSERT INTO #tblcheck2005to2008r2 
      EXEC master..sp_executesql @CMD 

      SELECT * 
      FROM   #tblcheck2005to2008r2 

      IF ( @IS_JOB_ACTION = 1 ) 
        BEGIN 
            DECLARE @MSGTEXT2 NVARCHAR(1200) 
            DECLARE cursoralerts CURSOR FOR 
              SELECT [messagetext] 
              FROM   #tblcheck2005to2008r2 

            OPEN cursoralerts 

            FETCH next FROM cursoralerts INTO @MSGTEXT2 

            WHILE @@FETCH_STATUS = 0 
              BEGIN 
                  IF NOT EXISTS(SELECT * 
                                FROM   [EZManagePro].[dbo].[et_alerts] 
                                WHERE  [alert_desc] = @MSGTEXT2) 
                    INSERT INTO [EZManagePro].[dbo].[et_alerts] 
                    VALUES      ('Integrity check job alert', 
                                 '1', 
                                 Getdate(), 
                                 NULL, 
                                 1, 
                                 @DB_NAME, 
                                 'Database Files', 
                                 @MSGTEXT2, 
                                 0, 
                                 0, 
                                 '', 
                                 0, 
                                 0, 
                                 3, 
                                 NULL, 
                                 NULL, 
                                 '1') 

                  FETCH next FROM cursoralerts INTO @MSGTEXT2 
              END 

            CLOSE cursoralerts 

            DEALLOCATE cursoralerts 
        END 

      IF EXISTS (SELECT [id] 
                 FROM   #tblcheck2005to2008r2) 
        BEGIN 
            DECLARE @NO_INFO_TEXT NVARCHAR(1200) 

            SET @NO_INFO_TEXT = '' 

            IF ( @NO_INFO_MSG = 0 ) 
              BEGIN 
                  SET @NO_INFO_TEXT = 'WITH NO_INFOMSGS' 
              END 

                  SET @CMD = 
                  N'ALTER DATABASE [' + @DB_NAME  + '] SET OFFLINE WITH ROLLBACK IMMEDIATE   
				  ALTER DATABASE [' + @DB_NAME + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE 
				  ALTER DATABASE [' + @DB_NAME +'] SET ONLINE WITH ROLLBACK IMMEDIATE  
				  DBCC CHECKDB (N''' + @DB_NAME + ''', ' + @REPAIR_TYPE + ')' 
                             + @NO_INFO_TEXT 

                  PRINT '@DB_NAME : ' + Cast(@DB_NAME AS NVARCHAR) 

                  EXEC master..sp_executesql @CMD 

                  SET @CMD = N'ALTER DATABASE [' + @DB_NAME + '] SET MULTI_USER WITH ROLLBACK IMMEDIATE'; 

                  EXEC master..sp_executesql @CMD 

                  IF @@ERROR <> 0 
                    BEGIN 
                        SET @CMD = N'ALTER DATABASE [' + @DB_NAME + '] SET MULTI_USER WITH ROLLBACK IMMEDIATE' 

                        EXEC master..sp_executesql 
                          @CMD 
                    END 
        END 
  END 
ELSE -- ALL other versions
  BEGIN 
      INSERT INTO #tblcheck 
      EXEC master..sp_executesql @CMD 

      SELECT * FROM   #tblcheck 

      IF ( @IS_JOB_ACTION = 1 ) 
        BEGIN 
            DECLARE @MSGTEXT3 NVARCHAR(1200) 
            DECLARE cursoralerts CURSOR FOR 
              SELECT [messagetext] 
              FROM   #tblcheck 

            OPEN cursoralerts 

            FETCH next FROM cursoralerts INTO @MSGTEXT3 

            WHILE @@FETCH_STATUS = 0 
              BEGIN 
                  IF NOT EXISTS(SELECT * 
                                FROM   [EZManagePro].[dbo].[et_alerts] 
                                WHERE  [alert_desc] = @MSGTEXT3) 
                    INSERT INTO [EZManagePro].[dbo].[et_alerts] 
                    VALUES      ('Integrity check job alert', 
                                 '1', 
                                 Getdate(), 
                                 NULL, 
                                 1, 
                                 @DB_NAME, 
                                 'Database Files', 
                                 @MSGTEXT3, 
                                 0, 
                                 0, 
                                 '', 
                                 0, 
                                 0, 
                                 3, 
                                 NULL, 
                                 NULL, 
                                 '1') 

                  FETCH next FROM cursoralerts INTO @MSGTEXT3 
              END 

            CLOSE cursoralerts 

            DEALLOCATE cursoralerts 
        END 

      IF not EXISTS (SELECT [dbid] 
                 FROM   #tblcheck) 
		print 'No corruption found'
	  ELSE
        BEGIN 
			print 'Found corruption, attempting to fix'
            DECLARE @NO_INFO_TEXT2 NVARCHAR(1200) 

            SET @NO_INFO_TEXT2 = '' 

            IF ( @NO_INFO_MSG = 0 ) 
              BEGIN 
                  SET @NO_INFO_TEXT2 = 'WITH NO_INFOMSGS' 
              END 

                SET @CMD = 
                N' ALTER DATABASE [' + @DB_NAME + '] SET OFFLINE WITH ROLLBACK IMMEDIATE
				ALTER DATABASE [' + @DB_NAME + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
				ALTER DATABASE [' + @DB_NAME + '] SET ONLINE WITH ROLLBACK IMMEDIATE
				DBCC CHECKDB (N''' + @DB_NAME + ''', ' + @REPAIR_TYPE + ')' + @NO_INFO_TEXT2 

                PRINT '@DB_NAME : ' + Cast(@DB_NAME AS NVARCHAR) 

                EXEC master..sp_executesql @CMD 

                SET @CMD = N'ALTER DATABASE [' + @DB_NAME + '] SET MULTI_USER WITH ROLLBACK IMMEDIATE'; 

                EXEC master..sp_executesql @CMD 

                IF @@ERROR <> 0 
                BEGIN 
                    SET @CMD = N'ALTER DATABASE [' + @DB_NAME + '] SET MULTI_USER WITH ROLLBACK IMMEDIATE' 

                    EXEC master..sp_executesql  @CMD 
                END 
        END 
  END