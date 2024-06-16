--EZMANAGE_
create FUNCTION [dbo].[FUNC_SELECT_BLOCK_CHILD](@id AS INT,
-- Old name: EZ_FN_SELECT_CHILD
@BlockAutoRefreshInterval INT,    -----the threshold cycle query sys.sysprocesses
           @OpenTransactionAlertMinLength INT,
           @BlockingAlertMinLength INT,
           @EnableOpenTransaction VARCHAR(5),
           @EnableBlocking VARCHAR(5))
RETURNS XML
--WITH ENCRYPTION 
AS
BEGIN
    RETURN  (
             SELECT session_id AS "@session_id",
                    blocking_session_id AS "@blocking_session_id",
                    root_session_id AS "@root_session_id",
                    duration_txt AS "@duration_txt",
                    CONVERT(VARCHAR,last_batch,20) AS "@last_batch",
                    login_name AS "@login_name",
                    host_name AS "@host_name",
                    program_name AS "@program_name",
                    login_db AS "@login_db",
                    CONVERT(VARCHAR,login_time,20) AS "@login_time",
                    status AS "@status",
                    open_tran AS "@open_tran",
                    wait_resource AS "@wait_resource",
                    cmd AS "@cmd",
                    start_offset AS "@start_offset",
                    len_offset AS "@len_offset",
                    dbo.FUNC_REMOVE_NULL_CHARS (batch_query) AS "@batch_query",
                    row_count AS "@row_count",
                    reads AS "@reads",
                    physical_reads AS "@physical_reads",
                    writes AS "@writes",
                    cpu AS "@cpu",
                    query_memory_kb  AS "@query_memory_kb",
                    isolation_level  AS "@isolation_level",
                    dbo.FUNC_SELECT_BLOCK_CHILD(session_id,
@BlockAutoRefreshInterval,    -----the threshold cycle query sys.sysprocesses
   @OpenTransactionAlertMinLength,
   @BlockingAlertMinLength,
   @EnableOpenTransaction ,
   @EnableBlocking)
                           FROM tempdb.dbo.EZ_MONITOR_BLOCKS_EXECUTE
                           WHERE blocking_session_id=@id
                           FOR XML PATH('process'), TYPE

    )
END