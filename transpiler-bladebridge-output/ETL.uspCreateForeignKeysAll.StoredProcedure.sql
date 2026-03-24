/****** Object:  StoredProcedure [ETL].[uspCreateForeignKeysAll]    Script Date: 03/03/2026 16:26:10 ******/




CREATE OR REPLACE PROCEDURE `ETL`.`uspCreateForeignKeysAll`(
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN


DECLARE VARIABLE V_Counter int
;
DECLARE VARIABLE V_Table STRING
;
DECLARE VARIABLE V_Schema STRING
;
DECLARE VARIABLE @;

CREATE TEMPORARY TABLE V_Temp
(RowNumber BIGINT GENERATED ALWAYS AS IDENTITY,TableName STRING,SchemaName STRING)
;
End int
;
SET V_Counter = 1

;
INSERT INTO V_Temp
(TableName,SchemaName)
(SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES
WHERE (TABLE_NAME LIKE 'Dim%'
OR TABLE_NAME LIKE 'Fact%')
AND NOT TABLE_SCHEMA = 'TMP')

SELECT @;
End = MAX(RowNumber) FROM V_Temp
;
WHILE V_Counter<@;
End
DO

SET V_Table = (
SELECT
TableName FROM V_Temp WHERE RowNumber = V_Counter
 LIMIT 1);
SET V_Schema = (
SELECT
SchemaName FROM V_Temp WHERE RowNumber = V_Counter
 LIMIT 1);
SELECT 'Searching Foreign Keys for ' ||V_Schema||'.'||V_Table
;
call ETL.uspCreateForeignKeysTable( V_Table, V_Schema, 1
);
SET V_Counter = V_Counter +1
;
END WHILE;
END
