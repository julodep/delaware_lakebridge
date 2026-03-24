/****** Object:  StoredProcedure [ETL].[SPR_UpsertData]    Script Date: 03/03/2026 16:26:10 ******/







CREATE OR REPLACE PROCEDURE `ETL`.`SPR_UpsertData`(
IN V_Environment STRING DEFAULT NULL,
IN V_TargetTable STRING,
IN V_ETLRunId INT,
IN V_TargetSchema STRING DEFAULT NULL)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_InsertCount INT
;
DECLARE VARIABLE V_UpdateCount INT
;
DECLARE VARIABLE V_DeleteCount INT
;
DECLARE VARIABLE V_SourceSchema STRING
;
DECLARE VARIABLE V_SourceTable STRING
;
DECLARE VARIABLE V_Query STRING
--Log Start  
;
CREATE TEMPORARY TABLE V_ETLRunDetailsId
(ETLRunDetailsId INT)
;
SELECT 'SPR_UpsertData: Log Start';
INSERT INTO ETL.RunDetail  
(  
TaskName,  
StartTime,  
NrOfIn, 
NrOfOut, 
NrOfNew, 
NrOfUpdates, 
NrOfDeletions, 
NrOfErrors,
ETLRunId  
) OUTPUT Inserted.ETLRunDetailsId INTO V_ETLRunDetailsId  
VALUES   
(  
'Upsert '||  V_TargetSchema || '.' || V_TargetTable,   
current_timestamp(),  
0,  
0,  
0,  
0,  
0, 
0, 
V_ETLRunId  
)

--Get Target information
;
SET V_SourceSchema = (
SELECT
SourceSchema FROM ETL.StagingInputTarget
WHERE TargetTable = V_TargetTable
	AND (V_TargetSchema IS NULL OR TargetSchema = V_TargetSchema)

 LIMIT 1);
SET V_SourceTable = (
SELECT
SourceTable FROM ETL.StagingInputTarget
WHERE TargetTable = V_TargetTable
	AND (V_TargetSchema IS NULL OR TargetSchema = V_TargetSchema)

 LIMIT 1);
SELECT 'Debug: TargetSchema = ' || V_TargetSchema
;
SELECT 'Debug: SourceSchema = ' || V_SourceSchema
;
SELECT 'Debug: SourceTable = ' || V_SourceTable


-- Build Query.
;
SET V_Query = 'call ETL.SPR_Upsert( ' || CHAR(10) ||
	'@SourceSchemaName = "' || V_SourceSchema || '",' || CHAR(10) ||
	'@SourceTableName = "' || V_SourceTable || '",' || CHAR(10) || 
	'@SchemaName = "' || V_TargetSchema || '",' || CHAR(10) || 
	'@TableName = "' || V_TargetTable || '",' || CHAR(10) || 
	'@ETLRunId = ' || CAST(V_ETLRunId AS STRING) || ',' || CHAR(10) || 
	'@InsertCount = @InsertCount ,' || CHAR(10) ||
	'@UpdateCount = @UpdateCount ,' || CHAR(10) || 
	'@DeleteCount = @DeleteCount '


);
SELECT CHAR(10) || 'Debug: Query: ' || CHAR(10) + COALESCE(V_Query, '') + CHAR(10)
;
EXECUTE IMMEADIATE V_Query, 
	'@InsertCount INT , @UpdateCount INT , @DeleteCount INT ',
	V_InsertCount=V_InsertCount , V_UpdateCount=V_UpdateCount , V_DeleteCount=V_DeleteCount 


--Log End
;
SELECT 'SPR_UpsertData: Log End';
UPDATE ETL.RunDetail  
SET EndTime = current_timestamp(),  
	NrOfNew = V_InsertCount,
	NrOfUpdates = V_UpdateCount,
	NrOfDeletions = V_DeleteCount,
	Status = 'Completed'  
WHERE ETLRunDetailsId = (SELECT MAX(ETLRunDetailsId) FROM V_ETLRunDetailsId)
;
END
