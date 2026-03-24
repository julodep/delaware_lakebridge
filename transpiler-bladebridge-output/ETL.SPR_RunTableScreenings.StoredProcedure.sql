/****** Object:  StoredProcedure [ETL].[SPR_RunTableScreenings]    Script Date: 03/03/2026 16:26:09 ******/





CREATE OR REPLACE PROCEDURE `ETL`.`SPR_RunTableScreenings`(
IN V_Schema STRING,
IN V_Table STRING,
IN V_ETLRunId int)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN


DECLARE VARIABLE V_RowsToProcess  int
;
DECLARE VARIABLE V_CurrentRow     int
;
DECLARE VARIABLE V_ScreeningExecutionId int
;
DECLARE VARIABLE V_ScreeningCategory STRING
;
DECLARE VARIABLE V_Query STRING
;
DECLARE VARIABLE V_Query2 STRING
;

SELECT '--Running screenings for ' ||V_Schema||'.'|| V_Table + CHAR(10)

;
SET V_Query =
'INSERT INTO ETL.ScreeningExecution (
	ScreeningId,
	StartTime,
	Status,
	ETLRunId,
	NrOfRows,
	NrOfErrors )
(
	SELECT ScreeningId, 
	current_timestamp() AS StartTime,
	"Created" AS Status,"'||
	CAST(V_ETLRunId AS STRING)||'" AS ETLRunId,
	0 AS NrOfRows,
	0 AS NrOfErrors
	FROM ETL.Screening
	WHERE TableName = "'||V_Table||'"
	AND SchemaName = "'||V_Schema||'"
	AND ScreeningActive =1
)'

;
EXECUTE (V_Query)

;
CREATE TEMPORARY TABLE TEMP_TABLE_Temp
(Id BIGINT GENERATED ALWAYS AS IDENTITY, ScreeningExecutionId int, ScreeningCategory STRING)

;
SET V_Query2 =
'INSERT INTO TEMP_TABLE_Temp
(ScreeningExecutionId, ScreeningCategory)
(
	SELECT ScreeningExecutionId, ScreeningCategory
	FROM ETL.ScreeningExecution SE
	INNER JOIN ETL.Screening S
	ON S.ScreeningId = SE.ScreeningId
	WHERE EndTime IS NULL AND ETLRunId = "'||CAST(V_ETLRunId AS STRING)||'"
)'

;
EXECUTE(V_Query2)

;
SET V_RowsToProcess=@@ROWCOUNT /*FIXME*/


;
SET V_CurrentRow=0
;
WHILE V_CurrentRow<V_RowsToProcess
DO
SET V_CurrentRow=V_CurrentRow+1;
SET V_ScreeningExecutionId = (
SELECT
ScreeningExecutionId FROM TEMP_TABLE_Temp WHERE Id = V_CurrentRow
 LIMIT 1);
SET V_ScreeningCategory = (
SELECT
ScreeningCategory FROM TEMP_TABLE_Temp WHERE Id = V_CurrentRow
 LIMIT 1);
IF V_ScreeningCategory = 'REFERENCE' THEN
EXECUTE ETL.SPR_ScreeningStructureFK V_ScreeningExecutionId ;
END WHILE;
IF V_ScreeningCategory = 'VALUE' THEN
EXECUTE ETL.SPR_ScreeningValue V_ScreeningExecutionId ;
END IF
;
END;
END
