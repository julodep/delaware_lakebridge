/****** Object:  View [DWH].[V_DimPurchaseOrder]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimPurchaseOrder` AS 


SELECT	DISTINCT 
		  UPPER(PurchaseOrderCode) AS PurchaseOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, SupplierCode

FROM DataStore2.PurchaseOrder

/* Create Unknown Member */

UNION ALL

SELECT DISTINCT 
		  '_N/A' AS PurchaseOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, '_N/A' AS SupplierCode

FROM DataStore.Company
;
