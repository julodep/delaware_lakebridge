/****** Object:  StoredProcedure [ETL].[SPR_MoveDataStoreData]    Script Date: 03/03/2026 16:26:09 ******/

















/* Create the procedure to move the data between the source system and the staging database*/
CREATE OR REPLACE PROCEDURE `ETL`.`SPR_MoveDataStoreData`(
IN V_DataStoreSchema STRING,
IN V_DataStoreTable STRING,
out V_NrOfRows INT)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_MoveDataStatement STRING
;
DECLARE VARIABLE V_StagingDatabase STRING
;
DECLARE VARIABLE V_TargetTable STRING
;
DECLARE VARIABLE V_TargetSchema STRING
;
DECLARE VARIABLE V_Prefix STRING
;
DECLARE VARIABLE V_TableColumnsStatement STRING
;
DECLARE VARIABLE V_TableColumns STRING
;
DECLARE VARIABLE V_ErrorMessage STRING
;
DECLARE VARIABLE V_MetadataDatabase STRING ;

SET V_MetadataDatabase = current_database()
--Get Input Parameters
;

SET V_StagingDatabase = (
SELECT
ConfiguredValue FROM ETL.S_ProjectParameters
WHERE ConfigurationFilter='DatabaseStaging'

 LIMIT 1);
IF (V_StagingDatabase IS NULL)
THEN
SET V_ErrorMessage = 'Missing SSISDB ProjectParameter for "DatabaseStaging"';
	THROW 51000, V_ErrorMessage, 1
;
END IF
;
SELECT 'Debug: StagingDatabase: '|| V_StagingDatabase
;
SET V_Prefix = (
SELECT
ConfiguredValue FROM ETL.S_ProjectParameters
WHERE ConfigurationFilter='Prefix'

 LIMIT 1);
IF (V_Prefix IS NULL)
THEN
SET V_ErrorMessage = 'Missing SSISDB ProjectParameter for "Prefix"';
	THROW 51000, V_ErrorMessage, 1
;
END IF
;
SELECT 'Debug: Prefix: '|| V_Prefix

-- Build TableColumns
;
SET V_TableColumnsStatement = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE `'||@StagingDatabase||'`'||CHAR(10) ELSE '' END ||
	'SELECT @TableColumns=COALESCE(@TableColumns||", ", "")||"`"||C.COLUMN_NAME ||"`"'||CHAR(10)||
	'FROM INFORMATION_SCHEMA.COLUMNS C '||CHAR(10)||
	'WHERE TABLE_SCHEMA=@DataStoreSchema AND TABLE_NAME=@DataStoreTable'
;
EXECUTE IMMEADIATE V_TableColumnsStatement, '@StagingDatabase STRING,@DataStoreSchema STRING,@DataStoreTable STRING,@TableColumns STRING ', 
	V_StagingDatabase=V_StagingDatabase,V_DataStoreSchema=V_DataStoreSchema,V_DataStoreTable=V_DataStoreTable,
	V_TableColumns=V_TableColumns 

;
IF (V_TableColumns IS NULL)
THEN
SET V_ErrorMessage = 'View "' || V_DataStoreSchema || '.' || V_Prefix+V_DataStoreTable || '" doesn"t exists';
	THROW 51000, V_ErrorMessage, 1
;
END IF

--Build MoveDataStatement
;
SET V_MoveDataStatement = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE ['+@StagingDatabase+']'+CHAR(10) ELSE '' END ||
'INSERT INTO `'||V_DataStoreSchema||'`.`'||V_DataStoreTable||'` ' ||CHAR(10)|| '(' || V_TableColumns || ')' ||CHAR(10)||
'SELECT ' ||CHAR(10)+ V_TableColumns +CHAR(10)|| 'FROM `'||V_DataStoreSchema||'`.`'||V_Prefix+V_DataStoreTable||'`  '

;
SELECT CHAR(10)||'Debug: MoveDataStatement = '|| V_MoveDataStatement
;
EXECUTE(V_MoveDataStatement)
;
SET V_NrOfRows = (
SELECT
@@ROWCOUNT /*FIXME*/  LIMIT 1);
SELECT 'Debug: RowCount = '||CAST(V_NrOfRows AS STRING)

;
RETURN V_NrOfRows
;
END
