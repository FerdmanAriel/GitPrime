--EZMANAGE_
CREATE FUNCTION [dbo].[GET_SERVER_SETTINGS]
(
@Setting_Name nvarchar(1000)
)
RETURNS nvarchar(1000)
AS
BEGIN
	DECLARE @res nvarchar(1000)

	SELECT top 1 @res = Value from EZManagePro.Settings.Server_Settings
	where [Key] = @Setting_Name

	-- Return the result of the function
	RETURN @res

END