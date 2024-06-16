--EZMANAGE_
CREATE PROCEDURE [dbo].[SP_CREATE_JOB]
-- Old name: EZME_CREATE_JOB
 @JOB_NAME NVARCHAR(256),
 @JOB_DESC NVARCHAR(512),
 @JOB_CATEGORY NVARCHAR(256),
 @STEP_NAME NVARCHAR(256),
 @STEP_SUBSYSTEM NVARCHAR(40),
 @STEP_COMMAND NVARCHAR(3200),
 @STEP_DATABASE NVARCHAR(128),
 @ON_FAIL_COMMAND NVARCHAR(3200),
 @ON_FAIL_SUBSYSTEM NVARCHAR(40),
 @ON_FAIL_DATABASE NVARCHAR(128),
 @ON_SUCCESS_COMMAND NVARCHAR(3200),
 @ON_SUCCESS_SUBSYSTEM NVARCHAR(40),
 @ON_SUCCESS_DATABASE NVARCHAR(128),
 @SCHEDULE_ENABLED BIT,
 @FREQ_TYPE INT,
 @FREQ_INTERVAL INT,
 @FREQ_SUBDAY_TYPE INT,
 @FREQ_SUBDAY_INTERVAL INT,
 @FREQ_RELATIVE_INTERVAL INT,
 @FREQ_RECURRENCE_FACTOR INT,
 @ACTIVE_START_DATE INT,
 @ACTIVE_END_DATE INT,
 @ACTIVE_START_TIME INT,
 @ACTIVE_END_TIME INT,
 @RETRY_ATTEMPTS INT = 0,
 @RETRY_INTERVAL INT = 0
--WITH ENCRYPTION
AS

SET NOCOUNT ON 

DECLARE @JOB_ID BINARY(16)
DECLARE @RETVAL INT
DECLARE @ERR NVARCHAR(512)
SET @RETVAL = 0

IF NOT EXISTS (SELECT * FROM msdb..syscategories WHERE [name] = @JOB_CATEGORY) EXEC msdb..sp_add_category @name = @JOB_CATEGORY

SELECT @JOB_ID = job_id FROM msdb..sysjobs WHERE [name] = @JOB_NAME
IF @JOB_ID IS NOT NULL
BEGIN
 IF EXISTS (SELECT * FROM msdb..sysjobservers WHERE [job_id] = @JOB_ID AND [server_id] != 0)
 BEGIN
  SET @ERR = N'Unable to create job '+@JOB_NAME+N' since there already is a multi-server job by that name'
  RAISERROR(@ERR, 16, 1)
  RETURN
 END
  ELSE
 BEGIN
  EXEC msdb..sp_delete_job @job_name = @JOB_NAME
  SET @JOB_ID = NULL
 END
END

EXEC @RETVAL = msdb..sp_add_job
 @job_id = @JOB_ID OUTPUT,
 @job_name = @JOB_NAME,
 @owner_login_name = N'',
 @description = @JOB_DESC,
 @category_name = @JOB_CATEGORY,
 @enabled = 1,
 @notify_level_email = 0,
 @notify_level_page = 0,
 @notify_level_netsend = 0,
 @notify_level_eventlog = 0,
 @delete_level = 0,
 @notify_email_operator_name = N''

EXEC @RETVAL = msdb..sp_add_jobserver
 @job_name = @JOB_NAME,
 @server_name = N'(LOCAL)'

DECLARE @STEP_ID INT
DECLARE @NEXT_STEP_FAIL_ACTION INT
DECLARE @NEXT_STEP_FAIL_ID INT
DECLARE @NEXT_STEP_SUCCEEDED_ACTION INT
DECLARE @NEXT_STEP_SUCCEEDED_ID INT
DECLARE @ON_FAIL_STEP_NAME NVARCHAR(256)
DECLARE @ON_SUCCESS_STEP_NAME NVARCHAR(256)

SET @ON_FAIL_STEP_NAME = @STEP_NAME+N'_FAIL'
SET @ON_SUCCESS_STEP_NAME = @STEP_NAME+N'_SUCCESS'

SELECT TOP 1 @STEP_ID = [step_id] FROM msdb..sysjobsteps WHERE job_id = @JOB_ID ORDER BY step_id DESC
SET @STEP_ID = ISNULL(@STEP_ID, 1)

IF (@ON_FAIL_COMMAND IS NULL) OR (@ON_FAIL_COMMAND = N'')
BEGIN
 SET @NEXT_STEP_FAIL_ID = 0
 SET @NEXT_STEP_FAIL_ACTION = 2
END
 ELSE
BEGIN
 SET @NEXT_STEP_FAIL_ID = @STEP_ID+1
 SET @NEXT_STEP_FAIL_ACTION = 4
END

IF (@ON_SUCCESS_COMMAND IS NULL) OR (@ON_SUCCESS_COMMAND = N'')
BEGIN
 SET @NEXT_STEP_SUCCEEDED_ID = 0
 SET @NEXT_STEP_SUCCEEDED_ACTION = 1
END
 ELSE
BEGIN
 IF @NEXT_STEP_FAIL_ID = 0 SET @NEXT_STEP_SUCCEEDED_ID = @STEP_ID+1 ELSE SET @NEXT_STEP_SUCCEEDED_ID = @NEXT_STEP_FAIL_ID+1
 SET @NEXT_STEP_SUCCEEDED_ACTION = 4
END

EXEC @RETVAL = msdb..sp_add_jobstep
 @job_name = @JOB_NAME,
 @step_id = @STEP_ID,
 @step_name = @STEP_NAME,
 @subsystem = @STEP_SUBSYSTEM,
 @command = @STEP_COMMAND,
 @on_success_action = @NEXT_STEP_SUCCEEDED_ACTION,
 @on_success_step_id = @NEXT_STEP_SUCCEEDED_ID,
 @on_fail_action = @NEXT_STEP_FAIL_ACTION,
 @on_fail_step_id = @NEXT_STEP_FAIL_ID,
 @database_name = @STEP_DATABASE,
 @retry_attempts = @RETRY_ATTEMPTS,
 @retry_interval = @RETRY_INTERVAL

IF @NEXT_STEP_FAIL_ID != 0
BEGIN
 EXEC @RETVAL = msdb..sp_add_jobstep
  @job_name = @JOB_NAME,
  @step_id = @NEXT_STEP_FAIL_ID,
  @step_name = @ON_FAIL_STEP_NAME,
  @subsystem = @ON_FAIL_SUBSYSTEM,
  @command = @ON_FAIL_COMMAND,
  @on_success_action = 2,
  @on_success_step_id = 0,
  @on_fail_action = 2,
  @on_fail_step_id = 0,
  @database_name = @ON_FAIL_DATABASE,
  @retry_attempts = @RETRY_ATTEMPTS,
  @retry_interval = @RETRY_INTERVAL
END

IF @NEXT_STEP_SUCCEEDED_ID != 0
BEGIN
 EXEC @RETVAL = msdb..sp_add_jobstep
  @job_name = @JOB_NAME,
  @step_id = @NEXT_STEP_SUCCEEDED_ID,
  @step_name = @ON_SUCCESS_STEP_NAME,
  @subsystem = @ON_SUCCESS_SUBSYSTEM,
  @command = @ON_SUCCESS_COMMAND,
  @on_success_action = 1,
  @on_success_step_id = 0,
  @on_fail_action = 1,
  @on_fail_step_id = 0,
  @database_name = @ON_SUCCESS_DATABASE,
  @retry_attempts = @RETRY_ATTEMPTS,
  @retry_interval = @RETRY_INTERVAL
END

DECLARE @SCHEDULE_NAME NVARCHAR(256)
SET @SCHEDULE_NAME = @JOB_NAME+N'_SCHED'

IF @SCHEDULE_ENABLED = 1
BEGIN
 EXEC @RETVAL = msdb..sp_add_jobschedule
  @job_name = @JOB_NAME,
  @name = @SCHEDULE_NAME,
  @enabled = 1,
  @freq_type = @FREQ_TYPE,
  @freq_interval = @FREQ_INTERVAL,
  @freq_subday_type = @FREQ_SUBDAY_TYPE,
  @freq_subday_interval = @FREQ_SUBDAY_INTERVAL,
  @freq_relative_interval = @FREQ_RELATIVE_INTERVAL,
  @freq_recurrence_factor = @FREQ_RECURRENCE_FACTOR,
  @active_start_date = @ACTIVE_START_DATE,
  @active_end_date = @ACTIVE_END_DATE,
  @active_start_time = @ACTIVE_START_TIME,
  @active_end_time = @ACTIVE_END_TIME
END