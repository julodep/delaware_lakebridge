/****** Object:  StoredProcedure [ETL].[CostSheetNodeHierarchy]    Script Date: 03/03/2026 16:26:09 ******/












CREATE OR REPLACE PROCEDURE `ETL`.`CostSheetNodeHierarchy`(
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_maxLevels INT
-- Retrieve Max Number of Levels From Hierarchy
;
DECLARE VARIABLE V_qLevel INT ;

DECLARE VARIABLE V_iLevel INT ;

DECLARE VARIABLE V_sqlSelectStmt STRING ;

DECLARE VARIABLE V_sqlFromStmt STRING ;

DECLARE VARIABLE V_finalSQL STRING
;
WITH depthCheck as
(
  SELECT H.NodeId, H.ParentNodeId, Code, CostGroupId, Description, DataAreaId, 0 AS Level
  FROM dbo.SMRBICostSheetNodeTableStaging H
  WHERE 1=1
	and H.ParentNodeId = 0
	and H.Level_ = 0

  UNION ALL

  SELECT Hc.NodeId, Hc.ParentNodeId, Hc.Code, Hc.CostGroupId, Hc.Description, Hc.DataAreaId, dc.Level + 1 AS Level
  FROM dbo.SMRBICostSheetNodeTableStaging Hc
  INNER JOIN depthCheck dC
  ON Hc.ParentNodeId = dC.NodeId
	and Hc.DataAreaId = dC.DataAreaId
 )

-- Add 1 Level because your column labels start with 1 instead of 0

--SELECT * FROM depthCheck

-- Check if table dbo.SMRBICostSheetNodeStaging contains rows
	-- if no rows default 3 (default in DataStore.V_ProductCostBreakdownHierarchy)
SET VARIABLE V_maxlevels = (SELECT  
		CASE  WHEN exists (select 1 from depthCheck) 
			 THEN MAX(Level) + 1
			 ELSE 3
		END 

FROM depthCheck
 limit 1);

SELECT V_maxLevels
--PRINT @maxLevels

-- Declare some variables needed to assemble the Dynamic SQL Statement
;
SET V_qLevel = V_maxLevels;
SET V_iLevel = V_maxLevels;
SET V_sqlSelectStmt = '';
SET V_sqlFromStmt = ''
--PRINT @sqlSelectStmt
--PRINT @sqlFromStmt
-- This nested While Loop builds out both custom SELECT and FROM clauses
;
WHILE V_qLevel > 0
DO
-- If Root level, the format will be different

IF V_qLevel = 1
    THEN
SET V_sqlSelectStmt = CHAR(9) + CHAR(32) || ', /\CostGroupId/\ AS Level_' || CAST(V_qLevel AS STRING) || '_CostGroupId' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
		;
SET V_sqlSelectStmt = CHAR(9) + CHAR(32) || ', /\Description/\ AS Level_' || CAST(V_qLevel AS STRING) || '_Description' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
		;
SET V_sqlSelectStmt = CHAR(9) + CHAR(32) || ', /\Code/\ AS Level_' || CAST(V_qLevel AS STRING) || '_Code' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
        ;
SET V_sqlFromStmt = CHAR(9) || 'dbo.SMRBICostSheetNodeTableStaging L' || CAST(V_qLevel AS STRING) + CHAR(13) + CHAR(10) + V_sqlFromStmt
    ;

    -- If not a Root Level, COALESCE statements will be required for the SELECT clause and JOINS will be required in the FROM clause
    ELSE


SET V_sqlSelectStmt = CHAR(9) + CHAR(32) || ', COALESCE(/\CostGroupId/\) AS Level_' || CAST(V_qLevel AS STRING) || '_CostGroupId' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
		;
SET V_sqlSelectStmt = CHAR(9) + CHAR(32) || ', COALESCE(/\Description/\) AS Level_' || CAST(V_qLevel AS STRING) || '_Description' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
		;
SET V_sqlSelectStmt = CHAR(9) + CHAR(32) || ', COALESCE(/\Code/\) AS Level_' || CAST(V_qLevel AS STRING) || '_Code' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
		;
SET V_sqlFromStmt = CHAR(9) || 'LEFT JOIN dbo.SMRBICostSheetNodeTableStaging L' || CAST(V_qLevel AS STRING) + CHAR(13) + CHAR(10) + CHAR(9) + CHAR(9) 
                         || 'ON L' || CAST(V_qLevel - 1 AS STRING) || '.NodeId = L' || CAST(V_qLevel AS STRING) || '.ParentNodeId' || CHAR(13) + CHAR(10) + CHAR(9) + CHAR(9) 
						 || 'and L' || CAST(V_qLevel - 1 AS STRING) || '.DataAreaId = L' || CAST(V_qLevel AS STRING) || '.DataAreaId' || CHAR(13) + CHAR(10) + V_sqlFromStmt
    ;
END WHILE IF

    -- Build out the cascading COALESCE statement for CODE and DESCRIPTION
;
WHILE V_iLevel > 0
    THEN
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, '/\CostGroupId/\', 'L' || CAST(V_iLevel AS STRING) || '.CostGroupId, /\CostGroupId/\')
		;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, '/\Description/\', 'L' || CAST(V_iLevel AS STRING) || '.Description, /\Description/\')
		;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, '/\Code/\', 'L' || CAST(V_iLevel AS STRING) || '.Code, /\Code/\')

		;
IF V_iLevel = 1
			;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, ', /\CostGroupId/\', '')
        ;
IF V_iLevel = 1
			;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, ', /\Description/\', '')
		;
IF V_iLevel = 1
			;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, ', /\Code/\', '')

        ;
SET V_iLevel = V_iLevel - 1
    ;
END IF

    -- Decriment @qLevel and Reset @iLevel counters
;
SET V_qLevel = V_qLevel - 1
    ;
SET V_iLevel = V_qLevel
;
END

-- Include the AccId Column
;
SET V_sqlSelectStmt = CHAR(13) + CHAR(9) || ' , COALESCE(/\ID/\) AS AccId' || CHAR(13) + CHAR(10) + V_sqlSelectStmt

-- Include the DataAreaId Column
;
SET V_sqlSelectStmt = CHAR(9) || ' UPPER(COALESCE(/\D_ID/\)) AS DataAreaId' || V_sqlSelectStmt

-- Build out the cascading COALESCE statement for the AccId column
;
SET V_iLevel = V_maxLevels
;
WHILE V_iLevel > 0
THEN
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, '/\ID/\', 'L' || CAST(V_iLevel AS STRING) || '.NodeId, /\ID/\')

    ;
IF V_iLevel = 1
        ;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, ', /\ID/\', '')

    ;
SET V_iLevel = V_iLevel - 1
;
END IF

-- Build out the cascading COALESCE statement for the DataAreaId column
;
SET V_iLevel = V_maxLevels
;
WHILE V_iLevel > 0
THEN
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, '/\D_ID/\', 'L' || CAST(V_iLevel AS STRING) || '.DataAreaId, /\D_ID/\')

    ;
IF V_iLevel = 1
        ;
SET V_sqlSelectStmt = REPLACE(V_sqlSelectStmt, ', /\D_ID/\', '')

    ;
SET V_iLevel = V_iLevel - 1
;
END IF

--PRINT @sqlSelectStmt
--PRINT @sqlFromStmt

-- Assemble final Dynamic SQL Statement
;
SET V_finalSQL = 'IF OBJECT_ID("dbo.CostSheetNodeHierarchyTable", "U") IS NOT NULL' || CHAR(13) + CHAR(10)
			  || 'DROP TABLE dbo.CostSheetNodeHierarchyTable' || CHAR(13)
			  || 'SELECT DISTINCT' || CHAR(13) + CHAR(10) + V_sqlSelectStmt
              || 'INTO dbo.CostSheetNodeHierarchyTable' || CHAR(13)
			  || 'FROM' || CHAR(13) + CHAR(10) + V_sqlFromStmt
              || 'WHERE 1=1' || CHAR(13) + CHAR(9) + CHAR(9)
			  || 'and L1.Level_ = 0' || CHAR(13) + CHAR(9) + CHAR(9)
			  || 'and L1.ParentNodeId = 0' || CHAR(13)

-- Print for good measure
;
SELECT V_finalSQL

-- Execute the assembled statement
;
EXECUTE IMMEADIATE V_finalSQL
;
END
