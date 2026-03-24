/****** Object:  StoredProcedure [ETL].[SPR_CreateDataStoreIndices]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE PROCEDURE `ETL`.`SPR_CreateDataStoreIndices`(
IN V_DataStoreSchema STRING,
IN V_DataStoreTable STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN



DECLARE VARIABLE V_RowsToProcess  INT
;
DECLARE VARIABLE V_CurrentRow     INT
;
DECLARE VARIABLE V_IndexName STRING
;
DECLARE VARIABLE V_Staging STRING ;

SET V_Staging = 'Staging'
;
CREATE TEMPORARY TABLE V_Indices
(Id BIGINT GENERATED ALWAYS AS IDENTITY, IndexName STRING)
;
INSERT INTO V_Indices 
(IndexName)
SELECT I.IndexName
FROM ETL.StagingInputIndex I
WHERE I.SourceName = V_Staging
	AND I.TargetSchema = V_DataStoreSchema
	AND I.TargetTable = V_DataStoreTable


;
SET V_RowsToProcess=V_V_ROWCOUNT

;
SET V_CurrentRow=0
;
WHILE V_CurrentRow<V_RowsToProcess
DO
SET V_CurrentRow=V_CurrentRow+1;
SET V_IndexName = (
SELECT
IndexName FROM V_Indices WHERE Id = V_CurrentRow
 LIMIT 1);
EXECUTE ETL.SPR_CreateStagingIndex V_IndexName = V_IndexName
;
END WHILE;
END
