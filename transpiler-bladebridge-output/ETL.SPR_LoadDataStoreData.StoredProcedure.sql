/****** Object:  StoredProcedure [ETL].[SPR_LoadDataStoreData]    Script Date: 03/03/2026 16:26:09 ******/






















/* Create the procedure to create views on the source database*/  
CREATE OR REPLACE PROCEDURE `ETL`.`SPR_LoadDataStoreData`(
IN V_DataStoreSchema STRING,
IN V_DataStoreTable STRING,
IN V_ETLRunId INT)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_NrOfRows int  
;
CREATE TEMPORARY TABLE V_ETLRunDetailId
(ETLRunDetailId int)  
BEGIN
TRAN 
--Log Start  
;
SELECT 'SPR_LoadDataStoreTable: Log Start';
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
) OUTPUT Inserted.ETLRunDetailsId INTO V_ETLRunDetailId  
VALUES   
(  
'Load '||V_DataStoreSchema || ' ' || V_DataStoreTable ||' to Data Store',    
current_timestamp(),  
0,  
0,  
0,  
0,  
0, 
0, 
V_ETLRunId  
)  
;
COMMIT
  
--Load Data Store Table  
;
SELECT 'SPR_LoadDataStoreTable: Stage Source Table'
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
GET DIAGNOSTICS CONDITION 1

		;
IF @@TRANCOUNT > 0 ;
ROLLBACK TRANSACTION
		SIGNAL SQLSTATE '45000';

END;

TRAN  
		;
SELECT 'SPR_LoadDataStoreTable: call SPR_CreateDataStoreTable('
		);
call ETL.SPR_CreateDataStoreTable( 
			V_DataStoreSchema = V_DataStoreSchema, 
			V_DataStoreTable  = V_DataStoreTable 

		);
SELECT 'SPR_LoadDataStoreTable: call SPR_MoveDataStoreData('
		);
call ETL.SPR_MoveDataStoreData( 
			V_DataStoreSchema = V_DataStoreSchema, 
			V_DataStoreTable = V_DataStoreTable, 
			V_NrOfRows = V_NrOfRows 

		);
SELECT 'SPR_LoadDataStoreTable: call SPR_CreateDataStoreIndices('
		);
call ETL.SPR_CreateDataStoreIndices( 
			V_DataStoreSchema = V_DataStoreSchema, 
			V_DataStoreTable = V_DataStoreTable 
	);
COMMIT TRAN
	;

END  
  
--Log End  
;
UPDATE ETL.RunDetail  
SET EndTime = current_timestamp(),  
	NrOfIn = COALESCE(V_NrOfRows, 0),  
	Status = 'Completed'  
WHERE ETLRunDetailsId = (SELECT MAX(ETLRunDetailId) FROM V_ETLRunDetailId)
;
END
