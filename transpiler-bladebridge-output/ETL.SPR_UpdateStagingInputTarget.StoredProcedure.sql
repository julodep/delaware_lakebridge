/****** Object:  StoredProcedure [ETL].[SPR_UpdateStagingInputTarget]    Script Date: 03/03/2026 16:26:09 ******/




CREATE OR REPLACE PROCEDURE `ETL`.`SPR_UpdateStagingInputTarget`(
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_SQL STRING
;
SET V_SQL = 'INSERT INTO ETL.StagingInputTarget (
		`TargetName`
		, `TargetSchema`
		, `TargetTable`
		, `SourceSchema`
		, `SourceTable`
		, `PrimaryKey`
		, `ActionType`
		, `Category`
		, `Status`
		, `Description`
		)
	SELECT	"DWH"
		, "DWH"
		, SUBSTRING(TABLE_NAME, 3, LEN(TABLE_NAME))
		, "DWH"
		, TABLE_NAME
		, NULL
		, "UPSERT"
		, NULL
		, "Active"
		, "Upsert to "|| SUBSTRING(TABLE_NAME, 3, LEN(TABLE_NAME))
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = "DWH"
		AND TABLE_NAME LIKE "V_DIM%"
		AND TABLE_NAME NOT IN (
			SELECT SourceTable
			FROM ETL.StagingInputTarget
			)'

			;
EXECUTE IMMEADIATE V_SQL
;
END
