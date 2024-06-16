--EZMANAGE_
create FUNCTION  [dbo].[FUNC_BIN_TO_HEX](@binvalue varbinary(256))
-- Old name: EZ_BIN_VALUE_TO_HEX_VALUE
RETURNS varchar (514)
--WITH ENCRYPTION
AS
BEGIN 
DECLARE @hexvalue varchar (514) 
DECLARE @charvalue varchar (514)
DECLARE @i int
DECLARE @length int
DECLARE @hexstring char(16)
SELECT @charvalue = '0x'
SELECT @i = 1
SELECT @length = DATALENGTH (@binvalue)
SELECT @hexstring = '0123456789ABCDEF'
WHILE (@i <= @length)
BEGIN
  DECLARE @tempint int
  DECLARE @firstint int
  DECLARE @secondint int
  SELECT @tempint = CONVERT(int, SUBSTRING(@binvalue,@i,1))
  SELECT @firstint = FLOOR(@tempint/16)
  SELECT @secondint = @tempint - (@firstint*16)
  SELECT @charvalue = @charvalue +
    SUBSTRING(@hexstring, @firstint+1, 1) +
    SUBSTRING(@hexstring, @secondint+1, 1)
  SELECT @i = @i + 1
END
SELECT @hexvalue = @charvalue
RETURN (@hexvalue)
END