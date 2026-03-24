/****** Object:  UserDefinedFunction [ETL].[fn_MonthKeyInt]    Script Date: 03/03/2026 16:26:08 ******/






CREATE FUNCTION `ETL`.`fn_MonthKeyInt`
(
	-- Add the parameters for the function here
	V_Date TIMESTAMP
)
RETURNS int
AS


RETURN COALESCE(CAST(RIGHT(CAST(EXTRACT(Year from V_Date) AS STRING),4)+RIGHT('0'||CAST(EXTRACT(Month from V_Date) AS STRING),2) AS Integer), 190001)
;
