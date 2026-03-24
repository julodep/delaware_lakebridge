/****** Object:  StoredProcedure [ETL].[SPR_CreateIndices]    Script Date: 03/03/2026 16:26:09 ******/



/* Create the procedure to create indices on the staging database*/
CREATE OR REPLACE PROCEDURE `ETL`.`SPR_CreateIndices`(
IN V_Environment STRING,
IN V_TargetName STRING,
IN V_TargetTable STRING,
IN V_TargetSchema STRING DEFAULT NULL)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN



DECLARE VARIABLE V_RowsToProcess  INT
;
DECLARE VARIABLE V_CurrentRow     INT
;
DECLARE VARIABLE V_Query STRING
;
DECLARE VARIABLE V_IndexName STRING
;

CREATE TEMPORARY TABLE TEMP_TABLE_Indices
(
	Id BIGINT GENERATED ALWAYS AS IDENTITY,
	IndexName STRING
)

;
SET V_Query = 
'INSERT INTO TEMP_TABLE_Indices (IndexName)
(SELECT I.IndexName
FROM ETL.StagingInputIndex I
WHERE I.SourceName = "' || V_TargetName || '"
	AND I.TargetTable = "' || V_TargetTable || '"
	AND ("' || COALESCE(V_TargetSchema, '') || '" = "" OR TargetSchema = "' || COALESCE(V_TargetSchema, '') || '")
)'

;
SELECT V_Query
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
SET V_IndexName = (
SELECT
IndexName FROM TEMP_TABLE_Indices WHERE Id = V_CurrentRow
 LIMIT 1);
EXECUTE ETL.SPR_CreateIndex V_Environment, V_IndexName
;
END WHILE

;
DROP TEMPORARY TABLE IF EXISTS TEMP_TABLE_Indices;
END
