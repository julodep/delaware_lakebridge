/****** Object:  StoredProcedure [ETL].[SPR_TruncateStagingEntity]    Script Date: 03/03/2026 16:26:09 ******/





/* Create the procedure to create views on the source database*/  
CREATE OR REPLACE PROCEDURE `ETL`.`SPR_TruncateStagingEntity`(
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_DynamicSQL  STRING ;

SET V_DynamicSQL = '';

SET V_DynamicSQL = (
SELECT
V_DynamicSQL || 'TRUNCATE TABLE ' || concat('[', SCHEMA_NAME(schema_id), ']') + concat('[', t.name, ']') || '; ' FROM sys.tables AS t

LEFT JOIN sys.partitions AS p
ON t.object_id = p.object_id
AND p.index_id IN ( 0, 1 )

WHERE SCHEMA_NAME(schema_id) = 'dbo'

GROUP BY SCHEMA_NAME(schema_id), t.name

HAVING SUM(p.rows) > 50000

--PRINT @DynamicSQL
 LIMIT 1);
EXECUTE IMMEADIATE V_DynamicSQL

;
END
