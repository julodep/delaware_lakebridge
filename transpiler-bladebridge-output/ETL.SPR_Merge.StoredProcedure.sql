/****** Object:  StoredProcedure [ETL].[SPR_Merge]    Script Date: 03/03/2026 16:26:09 ******/




/* It makes no sense to do SPR_Merge on a table having SCD2 columns because target contains versioning */
/* Columns of SCDType 'BK' should not be nullable or contain NULL values. */
CREATE OR REPLACE PROCEDURE `ETL`.`SPR_Merge`(
IN V_SourceSchemaName STRING,
IN V_SourceTableName STRING,
IN V_SchemaName STRING,
IN V_TableName STRING,
IN V_SourceDatabaseName STRING DEFAULT NULL,
IN V_TargetName STRING DEFAULT NULL,
IN V_MetadataDatabaseName STRING DEFAULT NULL,
IN V_ETLRunId INT DEFAULT NULL,
out V_InsertCount INT DEFAULT NULL,
out V_UpdateCount INT DEFAULT NULL,
out V_DeleteCount INT DEFAULT NULL)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_Statement STRING
;
DECLARE VARIABLE V_TargetLookups ETL.TargetLookup
--Get Columns
;
CREATE TEMPORARY TABLE V_Columns
(ColumnName STRING NOT NULL, SCDType STRING NOT NULL, DefaultDefinition STRING, PersistDefault BOOLEAN )
;
INSERT INTO V_Columns 
(ColumnName, SCDType, DefaultDefinition, PersistDefault)
SELECT C.name,
COALESCE(CAST(P.value AS STRING), 'SCD1') AS SCDType,
D.definition AS DefaultDefinition,
CAST(DP.value AS BOOLEAN) AS PersistDefault

FROM sys.columns c

INNER JOIN 
sys.tables T 
ON C.object_id = T.object_id

INNER JOIN
sys.schemas S
ON T.schema_id = S.schema_id

LEFT JOIN
sys.extended_properties P 
ON C.column_id = P.minor_id
AND C.object_id = P.major_id
AND  P.name = 'SCDType'

LEFT JOIN
sys.extended_properties DP
ON C.column_id = DP.minor_id
AND C.object_id = DP.major_id
AND  DP.name = 'MDM_PersistDefault'

LEFT JOIN
sys.default_constraints D
ON C.default_object_id = D.object_id

WHERE T.name =V_TableName
AND S.name = V_SchemaName
AND NOT COALESCE(P.value, 'SCD1') = 'PK' --If no 'SCDType' is defined, declare it as an SCD1
AND NOT C.is_computed = 1
AND C.generated_always_type = 0


-- Check for TargetLookups.
;
IF (V_MetadataDatabaseName IS NOT NULL AND V_TargetName IS NOT NULL)
THEN
SET V_Statement = 
		'SELECT LookupTableAlias, LookupSchema, LookupTable, LookupColumn, TargetColumn, SourceJoinColumns, LookupJoinColumns'||CHAR(10)||
		'FROM '||'`'||V_MetadataDatabaseName || '`.ETL.StagingInputTargetLookup'||CHAR(10)||
		'WHERE TargetName="'||V_TargetName||'" AND TargetSchema="'||V_SchemaName||'" AND TargetTable="'||V_TableName||'"'

;
	INSERT INTO V_TargetLookups 
	;
CALL V_Statement
	;
SELECT 'Debug: TargetLookup Query: ' || COALESCE(V_Statement, '')+CHAR(10)

	;
SET V_Statement = NULL;
SET V_Statement = (
SELECT
COALESCE(V_Statement+',', '')+LookupTableAlias FROM V_TargetLookups
	 LIMIT 1);
SELECT 'Debug: TargetLookups: ' || COALESCE(V_Statement, '')+CHAR(10)
	

;
END IF

;
SET V_Statement = 'DECLARE V_rowcounts TABLE(mergeAction STRING);'||CHAR(10)

--SCD1
;
SET V_Statement = 'MERGE INTO `'||V_SchemaName||'`.`'|| V_TableName || '` AS T'||CHAR(10)

;
IF EXISTS (SELECT 1 FROM V_TargetLookups) OR (V_ETLRunId IS NOT NULL AND EXISTS (SELECT 1 FROM V_Columns WHERE ColumnName = 'ETLRunId'))
THEN
SET V_Statement += 'USING'||CHAR(10)||'('||CHAR(10)+CHAR(9)||
		'SELECT'||CHAR(10)+CHAR(9)||'Source.*'

	;
IF EXISTS (SELECT 1 FROM V_TargetLookups)
	THEN
SELECT 'Merge Source will contain joins based on StagingInputTargetLookup';

		SELECT V_Statement += ',' || CHAR(10)+CHAR(9) + TL.LookupTableAlias || '.' || TL.LookupColumn +
			CASE WHEN TL.LookupColumn <> TL.TargetColumn THEN ' AS '+TL.TargetColumn ELSE '' END
		FROM V_TargetLookups TL
	;
END IF ;
IF (V_ETLRunId IS NOT NULL AND EXISTS (SELECT 1 FROM V_Columns WHERE ColumnName = 'ETLRunId'))
		SET V_Statement += ',' || CHAR(10)+CHAR(9)|| 'V_ETLRunId AS ETLRunId'
	
	SET V_Statement += CHAR(10)+CHAR(9)||'FROM '|| COALESCE('`'||V_SourceDatabaseName ||'`.', '') || '`' || V_SourceSchemaName || '`.' || COALESCE('`'||V_SourceTableName||'`', '`'||V_TableName||'`') || ' AS Source'

	;
IF EXISTS (SELECT 1 FROM V_TargetLookups)
	THEN
-- Remark: Use LEFT JOIN + add NOT NULL Constraint on LookupColumn(Alias) in Target: in case of mismatch by LEFT JOIN, MERGE should fail on NOT NULL Constraint.

		SELECT V_Statement += CHAR(10)+CHAR(9)||'LEFT JOIN `' || TL.LookupSchema || '`.`' || TL.LookupTable || '` AS ' || TL.LookupTableAlias
			+CHAR(10)+CHAR(9)+CHAR(9)||'ON ' || ETL.F_GetTargetLookupJoinClause(V_TargetLookups, TL.LookupTableAlias)
		FROM V_TargetLookups AS TL
	;
END IF

	SET V_Statement += CHAR(10)|| ') AS S'||CHAR(10)
;
END IF
ELSE
SET V_Statement += 'USING ' || COALESCE('`'||V_SourceDatabaseName ||'`.', '') || '`' || V_SourceSchemaName || '`.' || COALESCE('`'||V_SourceTableName||'`', '`'||V_TableName||'`') || ' AS S'||CHAR(10)

SET V_Statement += 'ON '
;
SET V_Statement = (
SELECT
V_Statement + 'T.['+ColumnName + '] = S.['+ColumnName+ '] AND'+CHAR(10) FROM V_Columns
WHERE SCDType= 'BK'

 LIMIT 1);
SET V_Statement = SUBSTRING(V_Statement,1,LEN(V_Statement)-4)

;
SET V_Statement = V_Statement +CHAR(10)+CHAR(10)|| 'WHEN MATCHED AND ('||CHAR(10)

;
SET V_Statement = V_Statement || 'NOT EXISTS ('||CHAR(10)||'SELECT'||CHAR(10);
SET V_Statement = (
SELECT
V_Statement + 'S.['+ColumnName+'],'+CHAR(10) FROM V_Columns
WHERE SCDType= 'SCD1'
 LIMIT 1);
SET V_Statement = SUBSTRING(V_Statement,1,LEN(V_Statement)-2)
+CHAR(10)

;
SET V_Statement = V_Statement || 'INTERSECT'||CHAR(10)||'SELECT'||CHAR(10);
SET V_Statement = (
SELECT
V_Statement + 'T.['+ColumnName+'],'+CHAR(10) FROM V_Columns
WHERE SCDType = 'SCD1'
 LIMIT 1);
SET V_Statement = SUBSTRING(V_Statement,1,LEN(V_Statement)-2) || ')'

;
SET V_Statement = V_Statement + CHAR(10) || ')'
||CHAR(10)+CHAR(10)|| 'THEN '||CHAR(10)

;
SET V_Statement = V_Statement || 'UPDATE'||CHAR(10)
||'SET'||CHAR(10)
;
SET V_Statement = (
SELECT
V_Statement +'T.['+ColumnName +'] = ' + CASE WHEN PersistDefault = 1 THEN DefaultDefinition ELSE 'S.['+ColumnName+']' END + ','+CHAR(10) FROM V_Columns

 LIMIT 1);
SET V_Statement = SUBSTRING(V_Statement,1,LEN(V_Statement)-2) + CHAR(10)
;
SET V_Statement = V_Statement +CHAR(10)+CHAR(10)|| 'WHEN NOT MATCHED BY TARGET'||CHAR(10)
||'THEN INSERT('||CHAR(10)
;
SET V_Statement = (
SELECT
V_Statement +'['+ColumnName +'],'+CHAR(10) FROM V_Columns

 LIMIT 1);
SET V_Statement = SUBSTRING(V_Statement,1,LEN(V_Statement)-2)+CHAR(10)
||')'||CHAR(10)
||'VALUES('||CHAR(10)

;
SET V_Statement = (
SELECT
V_Statement + COALESCE(DefaultDefinition, 'S.['+ColumnName+']') + ','+CHAR(10) FROM V_Columns

 LIMIT 1);
SET V_Statement = SUBSTRING(V_Statement,1,LEN(V_Statement)-2)+CHAR(10)
||')'||CHAR(10)

;
SET V_Statement = V_Statement || 'WHEN NOT MATCHED BY SOURCE THEN' || CHAR(10) || 'DELETE' || CHAR(10)

;
SET V_Statement = V_Statement|| 'OUTPUT $action INTO V_rowcounts;'||CHAR(10)||
'SELECT  V_InsertCount = `INSERT` , V_UpdateCount = `UPDATE` , V_DeleteCount = `DELETE`'||CHAR(10)||
'FROM (SELECT mergeAction, 1 ROWS FROM V_rowcounts) AS p'||CHAR(10)||
'PIVOT ( COUNT(ROWS)  FOR mergeAction IN (`INSERT`, `UPDATE`, `DELETE`) ) AS pvt;'||CHAR(10)||
'SELECT  COALESCE(V_InsertCount, 0) AS NrOfNew, COALESCE(V_UpdateCount, 0) AS NrOfChangingUpdates, COALESCE(V_DeleteCount, 0) AS NrOfDeletions'
;

SELECT V_Statement --Debugging
;
SELECT V_Statement
;
EXECUTE IMMEADIATE V_Statement, 
	'V_InsertCount INT , V_UpdateCount INT , V_DeleteCount INT , V_ETLRunId INT',
	V_InsertCount=V_InsertCount , V_UpdateCount=V_UpdateCount , V_DeleteCount=V_DeleteCount , V_ETLRunId=V_ETLRunId

;
SET V_InsertCount = COALESCE(V_InsertCount, 0)
;
SET V_UpdateCount = COALESCE(V_UpdateCount, 0)
;
SET V_DeleteCount = COALESCE(V_DeleteCount, 0)
;
END
