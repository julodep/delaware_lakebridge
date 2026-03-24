/****** Object:  StoredProcedure [ETL].[SPR_RunAllScreenings]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE PROCEDURE `ETL`.`SPR_RunAllScreenings`(
IN V_ETLRunId int,
IN V_SchemaName STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN


DECLARE VARIABLE V_RowsToProcess  int
;
DECLARE VARIABLE V_CurrentRow     int
;
DECLARE VARIABLE V_TableName STRING
;
DECLARE VARIABLE V_SchemaName2 STRING
;
DECLARE VARIABLE V_Query STRING
;

CREATE TEMPORARY TABLE TEMP_TABLE_Tables
(Id BIGINT GENERATED ALWAYS AS IDENTITY, TableName STRING,SchemaName STRING)

;
SET V_Query = 
'INSERT INTO TEMP_TABLE_Tables
(TableName,SchemaName)
(SELECT DISTINCT TableName,SchemaName
FROM ETL.Screening
WHERE (ScreeningActive = 1 OR ScreeningActive IS NULL) AND (SchemaName = "'||V_SchemaName||'" OR "'||V_SchemaName||'" = "All")
)'

;
EXECUTE (V_Query) 

;
SET V_RowsToProcess=@@ROWCOUNT /*FIXME*/

;
SET V_CurrentRow=0
;
WHILE V_CurrentRow<V_RowsToProcess
DO
SET V_CurrentRow=V_CurrentRow+1;
SET V_TableName = (
SELECT
TableName FROM TEMP_TABLE_Tables WHERE Id = V_CurrentRow
 LIMIT 1);
SET V_SchemaName2 = (
SELECT
SchemaName FROM TEMP_TABLE_Tables WHERE Id = V_CurrentRow
 LIMIT 1);
EXECUTE ETL.SPR_RunTableScreenings V_SchemaName2, V_TableName, V_ETLRunId
;
END WHILE;
END
