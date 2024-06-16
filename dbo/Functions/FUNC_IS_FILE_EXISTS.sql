--EZMANAGE_
create FUNCTION [dbo].[FUNC_IS_FILE_EXISTS](@path varchar(512))
-- Old name: fn_FileExists
RETURNS BIT
AS
BEGIN
     DECLARE @result INT
     EXEC master.dbo.xp_fileexist @path, @result OUTPUT
     RETURN cast(@result as bit)
END