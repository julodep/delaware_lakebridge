/****** Object:  View [DWH].[V_DimEmployee]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimEmployee` AS         


SELECT	  EmployeeId
		, UPPER(EmployeeCode) AS EmployeeCode
		, EmployeeName
		, EmployeeCodeName
		, DimensionName         

FROM DataStore.Employee

/* Create Unknown Member */

UNION ALL
       
SELECT -1
	  ,	'_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
;
