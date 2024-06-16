--EZMANAGE_
create FUNCTION [dbo].[FUNC_MS_TO_TEXT](@ms AS BIGINT)
-- Old name: EZ_FN_CONVERT_MS_TO_TEXT
  RETURNS VARCHAR(15)
  --WITH ENCRYPTION 
  AS
  BEGIN 
  RETURN  (
          SELECT 
           
							          CASE WHEN ABS(@ms)>=360000000 THEN 
									   RIGHT(
									          '000'+ (CONVERT(VARCHAR,CONVERT(BIGINT,ABS(@ms) /1000/60/60))),
									          3
									        ) 
									    ELSE        
									    
									    RIGHT(
									          '00'+ (CONVERT(VARCHAR,CONVERT(BIGINT,ABS(@ms) /1000/60/60))),
									          2
									        )
									    END +    
										':' + RIGHT 
										( 
											CONVERT(VARCHAR, DATEADD(SECOND, ABS(@ms) / 1000, 0), 120), 
											5 
										) + 
										'.' +RIGHT('000' + CONVERT(VARCHAR, ABS(@ms) % 1000), 3) 
		)								
		   
		 
END