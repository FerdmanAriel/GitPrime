--EZMANAGE_
create FUNCTION [dbo].[FUNC_REMOVE_NULL_CHARS]  ( @string NVARCHAR(MAX) ) 
-- Old name: EZ_REMOVE_NULL_CHARS
RETURNS NVARCHAR(MAX) 
WITH RETURNS NULL ON NULL INPUT-- ,ENCRYPTION
AS
BEGIN 
DECLARE @result NVARCHAR(MAX) 
SET @result = ''  DECLARE @counter INT  
SET @counter = 0  WHILE (@counter <= LEN(@string))     
BEGIN      
IF UNICODE(SUBSTRING(@string,@counter,1)) <>  0          
	SET @result = @result + SUBSTRING(@string,@counter,1)     
	SET @counter = @counter + 1         
END 
RETURN 
@result 
END