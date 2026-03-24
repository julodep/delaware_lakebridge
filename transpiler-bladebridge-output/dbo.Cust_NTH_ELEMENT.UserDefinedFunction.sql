/****** Object:  UserDefinedFunction [dbo].[Cust_NTH_ELEMENT]    Script Date: 03/03/2026 16:26:08 ******/




CREATE
	
 FUNCTION `dbo`.`Cust_NTH_ELEMENT` (
	V_Input STRING
	,V_Delim CHAR = '-'
	,V_N INT = 0
	)
RETURNS STRING
AS

	
RETURN (
			SELECT value
			FROM OPENJSON('``' || REPLACE(V_Input, V_Delim, '`,`') || '``')
			WHERE `key` = V_N
			)
;
