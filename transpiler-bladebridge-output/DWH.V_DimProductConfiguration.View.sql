/****** Object:  View [DWH].[V_DimProductConfiguration]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimProductConfiguration` AS


SELECT	  UPPER(CompanyCode) AS CompanyCode
		, UPPER(InventDimCode) AS InventDimCode
		, ProductConfigurationCode AS ProductConfigurationCode
		, InventBatchCode AS InventBatchCode
		, InventColorCode AS InventColorCode
		, InventSizeCode AS InventSizeCode
		, InventStyleCode AS InventStyleCode
		, InventStatusCode AS InventStatusCode
		, SiteCode as SiteCode
		, SiteName
		, WarehouseCode AS WarehouseCode
		, WarehouseName
		, WarehouseLocationCode AS WarehouseLocationCode

FROM DataStore.ProductConfiguration

UNION ALL

SELECT	DISTINCT UPPER(CompanyCode)
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'
		       , '_N/A'		       
FROM DataStore.Company
;
