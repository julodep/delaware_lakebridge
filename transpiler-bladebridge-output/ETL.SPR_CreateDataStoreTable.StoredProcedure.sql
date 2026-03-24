/****** Object:  StoredProcedure [ETL].[SPR_CreateDataStoreTable]    Script Date: 03/03/2026 16:26:09 ******/










/* Create the procedure to create tables on the staging database*/
CREATE OR REPLACE PROCEDURE `ETL`.`SPR_CreateDataStoreTable`(
IN V_DataStoreSchema STRING,
IN V_DataStoreTable STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_QueryStatement STRING
;
DECLARE VARIABLE V_CreateStagingTableColumns STRING
;
DECLARE VARIABLE V_Prefix STRING
;
DECLARE VARIABLE V_StagingDatabase STRING
;
DECLARE VARIABLE V_MetadataDatabase STRING ;

DECLARE VARIABLE V_PrimaryKey STRING
;
DECLARE VARIABLE V_CreateSchema INT
;
DECLARE VARIABLE V_ErrorMessage STRING
--Get Input Parameters
;
SET V_MetadataDatabase = current_database()
;

SET V_Prefix = (
SELECT
ConfiguredValue FROM ETL.S_ProjectParameters
WHERE ConfigurationFilter = 'Prefix'

 LIMIT 1);
IF (V_Prefix IS NULL)
THEN
SET V_ErrorMessage = 'Missing SSISDB ProjectParameter for "Prefix"';
	THROW 51000, V_ErrorMessage, 1
;
END IF
;
SELECT 'Debug: Prefix: '|| V_Prefix
;
SET V_StagingDatabase = (
SELECT
ConfiguredValue FROM ETL.S_ProjectParameters
WHERE ConfigurationFilter = 'DatabaseStaging'

 LIMIT 1);
IF (V_StagingDatabase IS NULL)
THEN
SET V_ErrorMessage = 'Missing SSISDB ProjectParameter for "DatabaseStaging"';
	THROW 51000, V_ErrorMessage, 1
;
END IF
;
SELECT 'Debug: StagingDatabase: '|| V_StagingDatabase

--Get PrimaryKey information
;
SET V_PrimaryKey = (
SELECT
PrimaryKey FROM ETL.StagingInputTarget
WHERE TargetName = 'Staging'
AND TargetSchema = V_DataStoreSchema
AND TargetTable = V_DataStoreTable

 LIMIT 1);
SELECT 'Debug: PrimaryKey = '||COALESCE(V_PrimaryKey, '')

--Build up Create Schema Statement for Staging Database
;
SET V_QueryStatement  = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE `'||@StagingDatabase||'`'||CHAR(10) ELSE '' END ||
'SELECT @CreateSchema = 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = "'||V_DataStoreSchema||'"'
;
EXECUTE IMMEADIATE V_QueryStatement, '@CreateSchema INT ',	V_CreateSchema=V_CreateSchema 
;
SET V_CreateSchema = (
SELECT
COALESCE(@@RowCount /*FIXME*/, 0)  LIMIT 1);
IF V_CreateSchema IS NULL
THEN
SELECT CHAR(10)||'Debug: CREATE SCHEMA '||V_DataStoreSchema
	;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
GET DIAGNOSTICS CONDITION 1

	;
SELECT 'Debug: Error in schema creation'
	;

END;
SET V_QueryStatement = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE ['+@StagingDatabase+'] ' ELSE '' END ||
		'CREATE SCHEMA '||V_DataStoreSchema|| ' AUTHORIZATION `db_ddladmin`'
	;
EXECUTE(V_QueryStatement)
	;

END IF

--Build Drop Data Store Table Statement
;
SET V_QueryStatement = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE ['+@StagingDatabase+']'+CHAR(10) ELSE '' END ||
'IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME="'||V_DataStoreTable||'" AND TABLE_SCHEMA = "'||V_DataStoreSchema||'")'||CHAR(10)||
'DROP TABLE `'||V_DataStoreSchema||'`.`'||V_DataStoreTable||'`'
;
SELECT CHAR(10)||'Debug: DropStagingTableStatement = '||V_QueryStatement
;
EXECUTE(V_QueryStatement)


--Build Create Data Store Table Statement
;
SET V_QueryStatement = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE ['+@StagingDatabase+']'+CHAR(10) ELSE '' END ||
	'SELECT @CreateStagingTableColumns=COALESCE(@CreateStagingTableColumns||", "||CHAR(10), "")||"`"||C.COLUMN_NAME||"`"|| " `"|| UPPER(C.DATA_TYPE)||"`"' || CHAR(10)||
		'|| CASE WHEN C.CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND C.DATA_TYPE <> "ntext" THEN "("||CASE WHEN C.CHARACTER_MAXIMUM_LENGTH = -1 THEN "MAX" ELSE CAST(C.CHARACTER_MAXIMUM_LENGTH AS STRING) END ||")" ELSE "" END' || CHAR(10)||
		'|| CASE WHEN C.DATA_TYPE IN ("decimal","numeric") THEN "("||CAST(C.NUMERIC_PRECISION AS STRING)||","||CAST(C.NUMERIC_SCALE AS STRING)||")" ELSE "" END' || CHAR(10)||
		'|| CASE WHEN C.IS_NULLABLE IS NOT NULL AND C.IS_NULLABLE = "NO" THEN " NOT NULL" ELSE " NULL" END ' || CHAR(10)||
	'FROM INFORMATION_SCHEMA.COLUMNS C ' || CHAR(10)||
	'WHERE C.TABLE_SCHEMA= @DataStoreSchema ' || CHAR(10)||
	'AND C.TABLE_NAME = @Prefix+@DataStoreTable ' || CHAR(10)||
	'ORDER BY C.ORDINAL_POSITION'
;
SELECT CHAR(10)|| 'Debug: @CreateStagingTableColumns = ' ||V_QueryStatement
;
EXECUTE IMMEADIATE V_QueryStatement,
	'@Prefix STRING, @DataStoreSchema STRING, @DataStoreTable STRING, @CreateStagingTableColumns STRING ',
	V_Prefix=V_Prefix, V_DataStoreSchema=V_DataStoreSchema, V_DataStoreTable=V_DataStoreTable, V_CreateStagingTableColumns=V_CreateStagingTableColumns 

;
IF (V_CreateStagingTableColumns IS NULL)
THEN
SET V_ErrorMessage = 'View "' || V_DataStoreSchema || '.' || V_Prefix+V_DataStoreTable || '" doesn"t exists';
	THROW 51000, V_ErrorMessage, 1
;
END IF

;
SET V_QueryStatement = CASE WHEN @StagingDatabase <> @MetadataDatabase THEN 'USE ['+@StagingDatabase+']'+CHAR(10) ELSE '' END ||
	'CREATE OR REPLACE TABLE `'||V_DataStoreSchema||'`.`'||V_DataStoreTable||'` ('||CHAR(10)+
	COALESCE(V_CreateStagingTableColumns, '')+
	COALESCE(','||CHAR(10)||'CONSTRAINT `PK_' || V_DataStoreSchema || '_' || V_DataStoreTable || '` PRIMARY KEY (' || V_PrimaryKey || ')', '')+CHAR(10)||
	')'
;
SELECT CHAR(10)||'Debug: CreateStagingTableStatement = '||COALESCE(V_QueryStatement, 'No Statement')
;
EXECUTE(V_QueryStatement)
;
END
