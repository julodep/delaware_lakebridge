/****** Object:  StoredProcedure [ETL].[SPR_SetSCDType]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE PROCEDURE `ETL`.`SPR_SetSCDType`(
IN V_SchemaName STRING,
IN V_TableName STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_CreateDescriptionPropertiesStatement STRING
;
SET V_CreateDescriptionPropertiesStatement = ''
;
SET V_CreateDescriptionPropertiesStatement = (
SELECT
V_CreateDescriptionPropertiesStatement +CHAR(10)||
'call sp_addextendedproperty( @name = "SCDType", @value = "'||
CASE WHEN C.COLUMN_NAME IN ('CreatedETLRunId','ModifiedETLRunId') THEN 'SCD0' 
		WHEN C.COLUMN_NAME IN ('SCDStartDate','SCDEndDate','SCDIsCurrent') THEN 'Historical'
		WHEN COLUMN_NAME = TABLE_NAME||'Id' THEN 'PK' 
		ELSE 'SCD1' END||'", 
@level0type = "Schema", @level0name = "'||C.TABLE_SCHEMA||'",
@level1type = "Table", @level1name = "'||C.TABLE_NAME||'",
@level2type = "Column", @level2name = "'||C.COLUMN_NAME||'");' FROM INFORMATION_SCHEMA.COLUMNS C
WHERE C.TABLE_NAME = V_TableName
AND C.TABLE_SCHEMA = V_SchemaName

 LIMIT 1);
SELECT CHAR(10)||'Debug: CreateDescriptionPropertiesStatement = '||V_CreateDescriptionPropertiesStatement+CHAR(10)
;
CALL V_CreateDescriptionPropertiesStatement
;
END
