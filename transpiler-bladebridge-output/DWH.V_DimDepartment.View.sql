/****** Object:  View [DWH].[V_DimDepartment]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimDepartment` AS         


SELECT    DepartmentId
		, UPPER(DepartmentCode) AS DepartmentCode
		, DepartmentName
		, DepartmentCodeName
		, DimensionName         

FROM DataStore.Department

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
