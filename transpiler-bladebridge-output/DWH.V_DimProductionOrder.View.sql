/****** Object:  View [DWH].[V_DimProductionOrder]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimProductionOrder` AS


SELECT	DISTINCT 
		  UPPER(ProductionOrderCode) AS ProductionOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, ProductionOrderStatus
		, ProductCode

FROM DataStore4.ProductionOrder


/* Create unknown member */

UNION ALL

SELECT	  '_N/A' AS ProductionOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, '_N/A' AS ProductionOrderStatusCode
		, '_N/A' AS ProductCode

FROM DataStore.Company
;
