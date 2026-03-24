/****** Object:  StoredProcedure [ETL].[SPR_CreateStagingIndex]    Script Date: 03/03/2026 16:26:09 ******/





CREATE OR REPLACE PROCEDURE `ETL`.`SPR_CreateStagingIndex`(
IN V_IndexName STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_StagingDatabase STRING
;
DECLARE VARIABLE V_TargetSchema STRING
;
DECLARE VARIABLE V_TargetTable STRING
;
DECLARE VARIABLE V_IsClustered BOOLEAN
;
DECLARE VARIABLE V_IsUnique BOOLEAN
;
DECLARE VARIABLE V_Columns STRING
;
DECLARE VARIABLE V_IncludeColumns STRING
;
DECLARE VARIABLE V_FilterPredicate STRING
;
DECLARE VARIABLE V_CreateStagingIndexStatement STRING
;
SELECT 'Debug: IndexName = '||V_IndexName

--Get Input Parameters
;
SET V_StagingDatabase = 'smartbi'
/*
SELECT @StagingDatabase=ConfiguredValue
FROM ETL.V_ProjectParameters
WHERE ConfigurationFilter = 'DatabaseStaging'
AND EnvironmentName = @Environment*/
;
SELECT 'Debug: StagingDatabase: '|| V_StagingDatabase
;
SET V_TargetSchema = (
SELECT
TargetSchema FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SET V_TargetTable = (
SELECT
TargetTable FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SET V_IsClustered = (
SELECT
IsClustered FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SET V_IsUnique = (
SELECT
IsUnique FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SET V_Columns = (
SELECT
Columns FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SET V_IncludeColumns = (
SELECT
IncludeColumns FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SET V_FilterPredicate = (
SELECT
FilteredPredicate FROM ETL.StagingInputIndex
WHERE IndexName = V_IndexName

 LIMIT 1);
SELECT 'Debug: TargetSchema = '|| V_TargetSchema
;
SELECT 'Debug: TargetTable = '|| V_TargetTable
;
SELECT 'Debug: IsClustered = '|| CAST(V_IsClustered AS STRING)
;
SELECT 'Debug: IsUnique = '|| CAST(V_IsUnique AS STRING)
;
SELECT 'Debug: Columns = '|| V_Columns
;
SELECT 'Debug: IncludeColumns = '|| COALESCE(V_IncludeColumns, '')
;
SELECT 'Debug: FilterPredicate = '|| COALESCE(V_FilterPredicate, '')

--Build Create Staging Table Statement
;
SET V_CreateStagingIndexStatement = 'USE `'||V_StagingDatabase||'`'||CHAR(10)||
	'CREATE ' ||
	CASE WHEN @IsUnique = 1 THEN 'UNIQUE ' ELSE '' END +
	CASE WHEN @IsClustered = 1 THEN 'CLUSTERED ' ELSE '' END ||
	'INDEX `' || V_IndexName || '` ON `' || V_TargetSchema || '`.`' || V_TargetTable || '` ' || 
	'(' || V_Columns || ') ' ||
	CASE WHEN @IncludeColumns IS NOT NULL THEN 'INCLUDE (' || @IncludeColumns || ') ' ELSE '' END +
	CASE WHEN @FilterPredicate IS NOT NULL THEN 'WHERE ' || @FilterPredicate ELSE '' END

;
SELECT CHAR(10)||'Debug: @CreateStagingIndexStatement = '||V_CreateStagingIndexStatement
;
EXECUTE(V_CreateStagingIndexStatement)
;
END
