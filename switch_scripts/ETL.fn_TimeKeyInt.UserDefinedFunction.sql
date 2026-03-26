/****** Object:  UserDefinedFunction [ETL].[fn_TimeKeyInt]    Script Date: 03/03/2026 16:26:08 ******/




CREATE FUNCTION `ETL`.`fn_TimeKeyInt`
(
	-- Add the parameters for the function here
	V_Time TIMESTAMP
)
RETURNS int
AS


RETURN CAST(CAST(EXTRACT(HOUR from V_Time) AS STRING)+ LEFT('0' || CAST(EXTRACT(MI from V_Time) AS STRING),2) AS INT)
;
