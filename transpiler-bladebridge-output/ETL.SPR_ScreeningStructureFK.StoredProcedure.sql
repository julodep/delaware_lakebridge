/****** Object:  StoredProcedure [ETL].[SPR_ScreeningStructureFK]    Script Date: 03/03/2026 16:26:09 ******/




CREATE OR REPLACE PROCEDURE `ETL`.`SPR_ScreeningStructureFK`(
IN V_ScreeningExecutionId int)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_Column STRING
;
DECLARE VARIABLE V_Table STRING
;
DECLARE VARIABLE V_RowIdColumns STRING
;
DECLARE VARIABLE V_RowIdValues STRING
;
DECLARE VARIABLE V_ErrorDetails STRING
;
DECLARE VARIABLE V_CorrectionValue STRING
;
DECLARE VARIABLE V_CorrectionType STRING
;
DECLARE VARIABLE V_Schema STRING
;
DECLARE VARIABLE V_TestColumn STRING
;
DECLARE VARIABLE V_ReferenceTable STRING
;
DECLARE VARIABLE V_ReferenceColumns STRING
;
DECLARE VARIABLE V_ReferenceSchema STRING
;
DECLARE VARIABLE V_CompanyCodeColumn STRING
;
DECLARE VARIABLE V_FromAndWhereClause STRING
;
DECLARE VARIABLE V_WhereClause STRING
;
DECLARE VARIABLE V_ErrorQuery STRING
;
DECLARE VARIABLE V_CorrectQuery STRING
;
DECLARE VARIABLE V_SourceColumns STRING
;
DECLARE VARIABLE V_CountQuery STRING
;
DECLARE VARIABLE V_NrOfRows int
;
DECLARE VARIABLE V_NrOfErrors int
;
DECLARE VARIABLE V_Query STRING
;
DECLARE VARIABLE V_Query2 STRING
;
SELECT '--Run Screening Execution '|| CAST(V_ScreeningExecutionId AS STRING)+ CHAR(10)

;
CREATE TEMPORARY TABLE TEMP_TABLE_NrOfRowsTable (NrOfRows INT)
;
CREATE TEMPORARY TABLE V_SourceTableJoinColumns
(
Id BIGINT GENERATED ALWAYS AS IDENTITY,
SourceJoinColumn STRING NOT NULL
)
;
CREATE TEMPORARY TABLE V_ReferenceTableJoinColumns
(
Id BIGINT GENERATED ALWAYS AS IDENTITY,
ReferenceJoinColumn STRING NOT NULL
)
;
CREATE TEMPORARY TABLE TEMP_TABLE_TableScreening
(`Column` STRING,
`Table` STRING,
`Schema` STRING,
ErrorDetails STRING,
CorrectionValue STRING,
ReferenceColumns STRING,
ReferenceTable STRING,
ReferenceSchema STRING,
WhereClause STRING,
CorrectionType STRING,
CompanyCodeColumn STRING,
RowIdColumns STRING
)

--Get screening values
;
SET V_Query2= 
'INSERT INTO TEMP_TABLE_TableScreening
(
`Column`,
`Table`,
`Schema`,
ErrorDetails,
CorrectionValue,
ReferenceColumns,
ReferenceTable,
ReferenceSchema,
WhereClause,
CorrectionType,
CompanyCodeColumn,
RowIdColumns
)
(SELECT
S.ColumnName,
S.TableName,
S.SchemaName,
S.ErrorDetails,
S.CorrectionValue,
S.ReferenceColumns,
S.ReferenceTable,
S.ReferenceSchema,
S.WhereClause,
S.CorrectionAction,
S.CompanyCode,
S.RowIdColumns

FROM ETL.ScreeningExecution SE
INNER JOIN ETL.Screening S 
ON SE.ScreeningId = S.ScreeningId
WHERE SE.ScreeningExecutionId = "'||CAST(V_ScreeningExecutionId AS STRING)||'"
AND (ScreeningActive = 1 OR ScreeningActive IS NULL))'

;
EXECUTE(V_Query2)
;
SET V_Column = (
SELECT
`Column` FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_Table = (
SELECT
`Table` FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_Schema = (
SELECT
`Schema` FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_ErrorDetails = (
SELECT
ErrorDetails FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_CorrectionValue = (
SELECT
CorrectionValue FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_ReferenceColumns = (
SELECT
ReferenceColumns FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_ReferenceTable = (
SELECT
ReferenceTable FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_ReferenceSchema = (
SELECT
ReferenceSchema FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_WhereClause = (
SELECT
WhereClause FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_CorrectionType = (
SELECT
CorrectionType FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_CompanyCodeColumn = (
SELECT
CompanyCodeColumn FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
SET V_RowIdColumns = (
SELECT
RowIdColumns FROM TEMP_TABLE_TableScreening



--Split the join columns from the source table
 LIMIT 1);
INSERT INTO V_SourceTableJoinColumns
(SourceJoinColumn)
(SELECT Value FROM `ETL`.fn_Split(V_Column,','))

--Split the join columns from the reference table
;
INSERT INTO V_ReferenceTableJoinColumns
(ReferenceJoinColumn)
(SELECT Value FROM `ETL`.fn_Split(V_ReferenceColumns,','))

--The first join column from the reference table is the column we will test for having the value NULL

;
SET V_TestColumn = (
SELECT
MAX(ReferenceJoinColumn) FROM V_ReferenceTableJoinColumns
--Prepare FROM and WHERE clause
 LIMIT 1);
SET V_FromAndWhereClause = 'FROM '||V_Schema||'.'||V_Table||' AS T1 '||CHAR(13)||
'LEFT JOIN ' || V_ReferenceSchema || '.' || V_ReferenceTable || ' AS T2 '|| CHAR(13)||
'ON '
;
SET V_FromAndWhereClause = (
SELECT
V_FromAndWhereClause + CASE WHEN T1.Id = 1 THEN CHAR(13) ELSE CHAR(13)+'AND ' END +'T1.'+ T1.SourceJoinColumn + '=T2.'+T2.ReferenceJoinColumn FROM V_SourceTableJoinColumns T1
INNER JOIN
V_ReferenceTableJoinColumns T2
ON T1.Id = T2.Id

 LIMIT 1);
SET V_FromAndWhereClause = V_FromAndWhereClause +CHAR(13)||
'WHERE T2.'||V_TestColumn ||' IS NULL '||CHAR(13)+
CASE WHEN V_WhereClause IS NULL THEN '' ELSE ' AND '+V_WhereClause END

;
SET V_RowIdValues = 'T1.'||V_RowIdColumns
;
SET V_RowIdValues = REPLACE(V_RowIdValues,',','||" - "'||'||T1.')

--Count all tested rows
;
SET V_CountQuery = 'INSERT INTO TEMP_TABLE_NrOfRowsTable(NrOfRows) (SELECT COUNT(*) FROM '||V_Schema||'.'||V_Table||' AS T1 '||CHAR(13)+CASE WHEN V_WhereClause IS NULL THEN '' ELSE 'WHERE '+V_WhereClause END||')'
;
EXECUTE(V_CountQuery);
SET V_NrOfRows = (
SELECT
COALESCE(MAX(NrOfRows), 0) FROM TEMP_TABLE_NrOfRowsTable
 LIMIT 1);
TRUNCATE TABLE TEMP_TABLE_NrOfRowsTable

--PRINT @FromAndWhereClause
--If we want to write away the incorrect rows in an error table
;
IF V_ErrorDetails ='ROW'
THEN

SET V_SourceColumns = (
SELECT
COALESCE(V_SourceColumns|| '|| " - "|| ','')+'T1.'+SourceJoinColumn FROM V_SourceTableJoinColumns
WHERE SourceJoinColumn <> COALESCE(V_CompanyCodeColumn, '')

 LIMIT 1);
SET V_ErrorQuery = 'INSERT INTO ETL.ScreeningError
(ErrorCode,
RowIdColumns,
RowIdValues,
ErrorColumns,
ErrorValues,
Occurence,
CompanyCode,
ScreeningExecutionId)

(SELECT 
"FK Constraint",'||CHAR(13)||
'"'||V_RowIdColumns||'",'||CHAR(13)+
V_RowIdValues||','||CHAR(13)||
'"'||V_Column||'",'||CHAR(13)+
V_SourceColumns||','||CHAR(13)||
'1,'||CHAR(13)+
COALESCE('T1.'||V_CompanyCodeColumn, '""')||','||CHAR(13)+
CAST(V_ScreeningExecutionId AS STRING)+CHAR(13)+
V_FromAndWhereClause||')'|| CHAR(13)||
'INSERT INTO TEMP_TABLE_NrOfRowsTable(NrOfRows) (SELECT COALESCE(V_V_ROWCOUNT, 0))'

;
SELECT '--Error Query:' || CHAR(10) +COALESCE(V_ErrorQuery, '') + CHAR(10)
;
EXECUTE (V_ErrorQuery)
;
END IF

--If we want to update the incorrect rows
;
IF V_CorrectionType = 'UPDATE'
THEN
SET V_CorrectQuery= 'UPDATE T1' || CHAR(13)||'SET'
;
SET V_CorrectQuery = (
SELECT
V_CorrectQuery + CASE WHEN T1.Id = 1 THEN CHAR(13) ELSE CHAR(13)+', ' END +'T1.'+ T1.SourceJoinColumn + CASE WHEN V_CorrectionValue = 'NULL' THEN '= NULL ' ELSE '= "'+V_CorrectionValue+'"' END FROM V_SourceTableJoinColumns T1
WHERE (SELECT COUNT(*) FROM V_SourceTableJoinColumns)=1 OR SourceJoinColumn <> V_CompanyCodeColumn

 LIMIT 1);
SET V_CorrectQuery = V_CorrectQuery+CHAR(13)+V_FromAndWhereClause+ CHAR(13)||
'INSERT INTO TEMP_TABLE_NrOfRowsTable(NrOfRows) (SELECT COALESCE(V_V_ROWCOUNT, 0))'

;
SELECT '--Correction Query: ' ||CHAR(10)+ COALESCE(V_CorrectQuery, '') + CHAR(10)
;
EXECUTE(V_CorrectQuery)
;
END IF

--If we want to delete the incorrect rows
;
IF V_CorrectionType = 'DELETE'
THEN
SET V_CorrectQuery= 'DELETE T1' || CHAR(13)+V_FromAndWhereClause+ CHAR(13)||
'INSERT INTO TEMP_TABLE_NrOfRowsTable(NrOfRows) (SELECT COALESCE(V_V_ROWCOUNT, 0))'
;
SELECT '--Correction Query: ' || CHAR(10) + COALESCE(V_CorrectQuery, '')+ CHAR(10)
;
EXECUTE(V_CorrectQuery)
;
END IF;
SET V_NrOfErrors = (
SELECT
COALESCE(MAX(NrOfRows), 0) FROM TEMP_TABLE_NrOfRowsTable
--Update the screening execution table
 LIMIT 1);
SET V_Query = 
'UPDATE ETL.ScreeningExecution
SET Status = CASE WHEN "'||CAST(V_NrOfErrors AS STRING)||'" =0 OR "'||V_ErrorDetails||'" = "NO" THEN "Completed" ELSE "Failed" END,
EndTime = current_timestamp(),
NrOfErrors = "'||CAST(V_NrOfErrors AS STRING)||'",
NrOfRows = "'||CAST(V_NrOfRows AS STRING)||'"
WHERE ScreeningExecutionId = "'||CAST(V_ScreeningExecutionId AS STRING)||'" '

;
EXECUTE(V_Query)

;
DROP TEMPORARY TABLE IF EXISTS TEMP_TABLE_NrOfRowsTable;
END
