/****** Object:  View [DWH].[V_DimCostCenter]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DWH`.`V_DimCostCenter` AS         


SELECT	  CostCenterId
		, CostCenterCode
		, CostCenterName
		, CostCenterCodeName
		, DimensionName         

FROM DataStore.CostCenter

/* Create Unknown Member */
UNION ALL
       
SELECT	-1
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
;
