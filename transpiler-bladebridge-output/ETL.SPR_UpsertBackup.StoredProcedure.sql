/****** Object:  StoredProcedure [ETL].[SPR_UpsertBackup]    Script Date: 03/03/2026 16:26:09 ******/





/*
Working with SCD2:
- Create your target table: @SchemaName.@TableName
	* Add an Identity column called @TableName + 'ID' (as first column)
	* Add SCDStartDate DATETIME, SCDEndDate DATETIME, SCDIsCurrent BIT at the end of the table
- Execute: EXEC SPR_SetSCDType @SchemaName, @TableName
	* Notice that column @TableName + 'ID' has now an extended property 'PK'
	* Notice that columns SCDStartDate, SCDEndDate, SCDIsCurrnet have now extended property 'Historical'
	* Modify the BusinessKey columns so the extended property 'SCDType' is set to 'BK'
	* Modify the SCD2 columns so the extended property 'SCDType' is set to 'SCD2'
- Create a helper table: @SourceSchemaName.@TableName+'2' (eg: stg.FactTable2)
	* Having the same table definition as the target table @SchemaName.@TableName
	* Except for the PK (@TableName + 'ID'), no need to add this column
*/

CREATE OR REPLACE PROCEDURE `ETL`.`SPR_UpsertBackup`(
IN V_SchemaName STRING,
IN V_TableName STRING,
IN V_SourceSchemaName STRING DEFAULT 'TMP',
IN V_SourceTableName STRING DEFAULT NULL,
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
DECLARE VARIABLE V_Statement1 STRING ;

DECLARE VARIABLE V_Statement2 STRING ;

DECLARE VARIABLE V_Statement3 STRING ;

DECLARE VARIABLE V_StatementDeclareRowCount STRING
;
DECLARE VARIABLE V_StatementRowCount STRING
;
DECLARE VARIABLE V_TargetLookupQuery STRING
;
DECLARE VARIABLE V_MaxId INT
;
DECLARE VARIABLE V_TargetLookups ETL.TargetLookup
--Get Columns
;
SET V_Statement1 = ''
;
SET V_Statement2 = ''
;
SET V_Statement3 = ''
;
CREATE TEMPORARY TABLE V_Columns
(ColumnName STRING NOT NULL, SCDType STRING NOT NULL, DefaultDefinition STRING, PersistDefault BOOLEAN )
;
INSERT INTO V_Columns 
(ColumnName, SCDType, DefaultDefinition, PersistDefault)
SELECT C.name AS ColumnName,
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
AND DP.name = 'MDM_PersistDefault'

LEFT JOIN
sys.default_constraints D
ON C.default_object_id = D.object_id

WHERE T.name =V_TableName
AND S.name = V_SchemaName
AND NOT COALESCE(P.value, 'SCD1') = 'PK' --If no 'SCDType' is defined, declare it as an SCD1
AND NOT C.is_computed = 1
AND C.generated_always_type = 0

;
SET V_StatementDeclareRowCount = 'DECLARE V_rowcounts TABLE(mergeAction STRING);'||CHAR(10)

;
SET V_StatementRowCount = 'OUTPUT $action INTO V_rowcounts;'||CHAR(10)||
'SELECT  V_InsertCount = `INSERT` , V_UpdateCount = `UPDATE` , V_DeleteCount = `DELETE`'||CHAR(10)||
'FROM (SELECT mergeAction, 1 ROWS FROM V_rowcounts) AS p'||CHAR(10)||
'PIVOT ( COUNT(ROWS)  FOR mergeAction IN (`INSERT`, `UPDATE`, `DELETE`) ) AS pvt;'||CHAR(10)||
'SELECT  COALESCE(V_InsertCount, 0) AS NrOfNew, COALESCE(V_UpdateCount, 0) AS NrOfChangingUpdates, COALESCE(V_DeleteCount, 0) AS NrOfDeletions'


-- Check for TargetLookups.
;
IF (V_MetadataDatabaseName IS NOT NULL AND V_TargetName IS NOT NULL)
THEN
SET V_TargetLookupQuery = 
		'SELECT LookupTableAlias, LookupSchema, LookupTable, LookupColumn, TargetColumn, SourceJoinColumns, LookupJoinColumns'||CHAR(10)||
		'FROM '||'`'||V_MetadataDatabaseName || '`.ETL.StagingInputTargetLookup'||CHAR(10)||
		'WHERE TargetName="'||V_TargetName||'" AND TargetSchema="'||V_SchemaName||'" AND TargetTable="'||V_TableName||'"'

;
	INSERT INTO V_TargetLookups 
	;
CALL V_TargetLookupQuery
	;
SELECT 'Debug: TargetLookup Query: ' || COALESCE(V_TargetLookupQuery, '')+CHAR(10)

	;
SET V_TargetLookupQuery = NULL;
SET V_TargetLookupQuery = (
SELECT
COALESCE(V_TargetLookupQuery+',', '')+LookupTableAlias FROM V_TargetLookups
	 LIMIT 1);
SELECT 'Debug: TargetLookups: ' || COALESCE(V_TargetLookupQuery, '')+CHAR(10)
	

;
END IF

--SCD1
;
SET V_Statement1 = 'MERGE INTO `'||V_SchemaName||'`.`'|| V_TableName || '` AS T'||CHAR(10)

;
IF EXISTS (SELECT 1 FROM V_TargetLookups) OR (V_ETLRunId IS NOT NULL AND EXISTS (SELECT 1 FROM V_Columns WHERE ColumnName = 'ETLRunId'))
THEN
SET V_Statement1 += 'USING'||CHAR(10)||'('||CHAR(10)+CHAR(9)||
		'SELECT'||CHAR(10)+CHAR(9)||'Source.*'

	;
IF EXISTS (SELECT 1 FROM V_TargetLookups)
	THEN
SELECT 'Merge Source will contain joins based on StagingInputTargetLookup';

		SELECT V_Statement1 += ',' || CHAR(10)+CHAR(9) + TL.LookupTableAlias || '.' || TL.LookupColumn +
			CASE WHEN TL.LookupColumn <> TL.TargetColumn THEN ' AS '+TL.TargetColumn ELSE '' END
		FROM V_TargetLookups TL
	;
END IF ;
IF (V_ETLRunId IS NOT NULL AND EXISTS (SELECT 1 FROM V_Columns WHERE ColumnName = 'ETLRunId'))
		SET V_Statement1 += ',' || CHAR(10)+CHAR(9)|| 'V_ETLRunId AS ETLRunId'
	
	SET V_Statement1 += CHAR(10)+CHAR(9)||'FROM '|| COALESCE('`'||V_SourceDatabaseName ||'`.', '') || '`' || V_SourceSchemaName || '`.' || COALESCE('`'||V_SourceTableName||'`', '`'||V_TableName||'`') || ' AS Source'

	;
IF EXISTS (SELECT 1 FROM V_TargetLookups)
	THEN
-- Remark: Use LEFT JOIN + add NOT NULL Constraint on LookupColumn(Alias) in Target: in case of mismatch by LEFT JOIN, MERGE should fail on NOT NULL Constraint.

		SELECT V_Statement1 += CHAR(10)+CHAR(9)||'LEFT JOIN `' || TL.LookupSchema || '`.`' || TL.LookupTable || '` AS ' || TL.LookupTableAlias
			+CHAR(10)+CHAR(9)+CHAR(9)||'ON ' || ETL.F_GetTargetLookupJoinClause(V_TargetLookups, TL.LookupTableAlias)
		FROM V_TargetLookups AS TL
	;
END IF

	SET V_Statement1 += CHAR(10)|| ') AS S'||CHAR(10)
;
END IF
ELSE
SET V_Statement1 += 'USING ' || COALESCE('`'||V_SourceDatabaseName ||'`.', '') || '`' || V_SourceSchemaName || '`.' || COALESCE('`'||V_SourceTableName||'`', '`'||V_TableName||'`') || ' AS S'||CHAR(10)

SET V_Statement1 += 'ON '
;
SET V_Statement1 = (
SELECT
V_Statement1 + 'T.['+ColumnName + '] = S.['+ColumnName+ '] AND'+CHAR(10) FROM V_Columns
WHERE SCDType= 'BK'

 LIMIT 1);
SET V_Statement1 = SUBSTRING(V_Statement1,1,LEN(V_Statement1)-4)

;
SET V_Statement1 = V_Statement1 +CHAR(10)+CHAR(10)|| 'WHEN MATCHED AND ('||CHAR(10)

;
SET V_Statement2 = V_Statement2 || 'NOT EXISTS ('||CHAR(10)||'SELECT'||CHAR(10);
SET V_Statement2 = (
SELECT
V_Statement2 + 'S.['+ColumnName+'],'+CHAR(10) FROM V_Columns
WHERE SCDType= 'SCD1'
 LIMIT 1);
SET V_Statement2 = SUBSTRING(V_Statement2,1,LEN(V_Statement2)-2)
+CHAR(10)

;
SET V_Statement2 = V_Statement2 || 'INTERSECT'||CHAR(10)||'SELECT'||CHAR(10);
SET V_Statement2 = (
SELECT
V_Statement2 + 'T.['+ColumnName+'],'+CHAR(10) FROM V_Columns
WHERE SCDType = 'SCD1'
 LIMIT 1);
SET V_Statement2 = SUBSTRING(V_Statement2,1,LEN(V_Statement2)-2) || ')'

;
SET V_Statement2 = V_Statement2 + CHAR(10) || ')'
||CHAR(10)+CHAR(10)|| 'THEN '||CHAR(10)


;
SET V_Statement3 = 'UPDATE'||CHAR(10)
||'SET'||CHAR(10)
;
SET V_Statement3 = (
SELECT
V_Statement3 +'T.['+ColumnName +'] = ' + CASE WHEN PersistDefault = 1 THEN DefaultDefinition ELSE 'S.['+ColumnName+']' END + ','+CHAR(10) FROM V_Columns
WHERE SCDType IN ('SCD0', 'SCD1')	-- Also include SCDType = SCD0, even if there is a default for update statement, but take into account PersistDefault.
 LIMIT 1);
SET V_Statement3 = SUBSTRING(V_Statement3,1,LEN(V_Statement3)-2)

;
SET V_Statement3 = V_Statement3 +CHAR(10)+CHAR(10)|| 'WHEN NOT MATCHED BY TARGET'||CHAR(10)
||'THEN INSERT('||CHAR(10)
;
SET V_Statement3 = (
SELECT
V_Statement3 +'['+ColumnName +'],'+CHAR(10) FROM V_Columns

 LIMIT 1);
SET V_Statement3 = SUBSTRING(V_Statement3,1,LEN(V_Statement3)-2)+CHAR(10)
||')'||CHAR(10)
||'VALUES('||CHAR(10)

;
SET V_Statement3 = (
SELECT
V_Statement3 + COALESCE(DefaultDefinition, 'S.['+ColumnName+']') + ','+CHAR(10) FROM V_Columns
WHERE SCDType <> 'Historical'
	
 LIMIT 1);
IF ((SELECT COUNT(*) FROM V_Columns WHERE SCDType='SCD2')>0)
THEN
SET V_Statement3 = V_Statement3 || 'current_timestamp(),' || CHAR(10)
||'NULL,'||CHAR(10)
||'1' || CHAR(10) || ','
;
END IF

;
SET V_Statement3 = SUBSTRING(V_Statement3,1,LEN(V_Statement3)-2)+CHAR(10)
||')'||CHAR(10)

;
SET V_Statement = V_StatementDeclareRowCount + V_Statement1 + V_Statement2 + V_Statement3 + V_StatementRowCount
;
SELECT V_Statement;

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


--SCD2
;
SET V_Statement1= ''
;
SET V_Statement2 = ''
;
SET V_Statement3 = ''
;
IF ((SELECT COUNT(*) FROM V_Columns WHERE SCDType='SCD2')>0)
THEN
SELECT '--SCD2 Detected';

SELECT ('TRUNCATE TABLE '||V_SourceSchemaName||'.'|| V_TableName||'2')
;
EXECUTE('TRUNCATE TABLE '||V_SourceSchemaName||'.'|| V_TableName||'2')
;
SET V_Statement1 = 'INSERT INTO '||V_SourceSchemaName||'.'|| V_TableName||'2('||CHAR(10)
;
SET V_Statement1 = (
SELECT
V_Statement1 +'['+ColumnName +'],'+CHAR(10) FROM V_Columns
WHERE SCDType <> 'Historical'

 LIMIT 1);
SET V_Statement1 = V_Statement1 || 'SCDStartDate,' || CHAR(10)
||'SCDEndDate,'||CHAR(10)
||'SCDIsCurrent)'||CHAR(10)

;
SET V_Statement1 = V_Statement1 || 'SELECT '||CHAR(10);
SET V_Statement1 = (
SELECT
V_Statement1 +'['+ColumnName +'],'+CHAR(10) FROM V_Columns
WHERE SCDType <> 'Historical'

 LIMIT 1);
SET V_Statement1 = V_Statement1 || 'current_timestamp(),' || CHAR(10)
||'NULL,'||CHAR(10)
||'1'||CHAR(10)
||'FROM ('||CHAR(10)

;
SET V_Statement1 = V_Statement1 || 'MERGE INTO '||V_SchemaName||'.'|| V_TableName || ' AS T'||CHAR(10)
||'USING ' || V_SourceSchemaName || '.'||V_TableName||' AS S'||CHAR(10)
||'ON '
;
SET V_Statement1 = (
SELECT
V_Statement1 + 'T.['+ColumnName + '] = S.['+ColumnName+ '] AND'+CHAR(10) FROM V_Columns
WHERE SCDType= 'BK'

 LIMIT 1);
SET V_Statement1 = SUBSTRING(V_Statement1,1,LEN(V_Statement1)-4)

;
SET V_Statement1 = V_Statement1 +CHAR(10)+CHAR(10)|| 'WHEN MATCHED AND ('||CHAR(10)

;
SET V_Statement2 = V_Statement2 || 'NOT EXISTS ('||CHAR(10)||'SELECT'||CHAR(10);
SET V_Statement2 = (
SELECT
V_Statement2 + 'S.['+ColumnName+'],'+CHAR(10) FROM V_Columns
WHERE SCDType= 'SCD2'
 LIMIT 1);
SET V_Statement2 = SUBSTRING(V_Statement2,1,LEN(V_Statement2)-2)
+CHAR(10)

;
SET V_Statement2 = V_Statement2 || 'INTERSECT'||CHAR(10)||'SELECT'||CHAR(10);
SET V_Statement2 = (
SELECT
V_Statement2 + 'T.['+ColumnName+'],'+CHAR(10) FROM V_Columns
WHERE SCDType = 'SCD2'
 LIMIT 1);
SET V_Statement2 = SUBSTRING(V_Statement2,1,LEN(V_Statement2)-2) || ')'

;
SET V_Statement2 = V_Statement2 + CHAR(10) || ') AND T.SCDIsCurrent = 1'
||CHAR(10)+CHAR(10)|| 'THEN '||CHAR(10)

;
SET V_Statement3 = 'UPDATE'||CHAR(10)
||'SET'||CHAR(10)
||'SCDIsCurrent = 0,'||CHAR(10)
||'SCDEndDate = current_timestamp()' || CHAR(10)
||'OUTPUT $action AS Action,'||CHAR(10)
||'S.* ) AS MergeOutput'||CHAR(10)
||'WHERE MergeOutput.Action = "UPDATE"'

;
SELECT V_Statement1 + V_Statement2 + V_Statement3;

SELECT (V_Statement1 + V_Statement2 + V_Statement3)
;
EXECUTE(V_Statement1 + V_Statement2 + V_Statement3)

;
SET V_Statement1 = 'INSERT INTO '||V_SchemaName||'.'|| V_TableName||'('||CHAR(10)
;
SET V_Statement1 = (
SELECT
V_Statement1 +'['+ColumnName +'],'+CHAR(10) FROM V_Columns
WHERE SCDType <> 'Historical'

 LIMIT 1);
SET V_Statement1 = V_Statement1 || 'SCDStartDate,' || CHAR(10)
||'SCDEndDate,'||CHAR(10)
||'SCDIsCurrent)'||CHAR(10)

;
SET V_Statement1 = V_Statement1|| '(SELECT '||CHAR(10);
SET V_Statement1 = (
SELECT
V_Statement1 +'['+ColumnName +'],'+CHAR(10) FROM V_Columns
WHERE SCDType <> 'Historical'

 LIMIT 1);
SET V_Statement1 = V_Statement1 || 'current_timestamp(),' || CHAR(10)
||'NULL,'||CHAR(10)
||'1'||CHAR(10)
||'FROM '||V_SourceSchemaName||'.'||V_TableName||'2)'

;
SELECT(V_Statement1);

SELECT(V_Statement1)
;
EXECUTE(V_Statement1)

;
SET V_Statement2 = 'SELECT Count(1) AS NrOfSCD2Changes FROM '||V_SourceSchemaName||'.'||V_TableName||'2'
;
SELECT(V_Statement2);

SELECT(V_Statement2)
;
EXECUTE(V_Statement2)

;
END IF;
END
