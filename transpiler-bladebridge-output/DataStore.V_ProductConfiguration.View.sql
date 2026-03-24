/****** Object:  View [DataStore].[V_ProductConfiguration]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore`.`V_ProductConfiguration` AS


SELECT	UPPER(IDS.DataAreaId) AS CompanyCode
	  , IDS.InventDimId AS InventDimCode
	  , COALESCE(NULLIF(IDS.ConfigId, ''), '_N/A') AS ProductConfigurationCode
	  , COALESCE(NULLIF(UPPER(IDS.InventBatchId), ''), '_N/A') AS InventBatchCode
	  , COALESCE(NULLIF(IDS.InventColorId, ''), '_N/A') AS InventColorCode
	  , COALESCE(NULLIF(IDS.InventSizeId, ''), '_N/A') AS InventSizeCode
	  , COALESCE(NULLIF(IDS.InventStyleId, ''), '_N/A') AS InventStyleCode
	  , COALESCE(NULLIF(IDS.InventStatusId, ''), '_N/A') AS InventStatusCode
	  , COALESCE(NULLIF(IDS.InventSiteId, ''), '_N/A') AS SiteCode
	  , COALESCE(UPPER(ISS.Name), '_N/A') AS SiteName
	  , COALESCE(NULLIF(IDS.InventLocationId, ''), '_N/A') AS WarehouseCode
	  , COALESCE(UPPER(IWS.WarehouseName), '_N/A') AS WarehouseName
	  , COALESCE(NULLIF(IDS.WMSLocationId, ''), '_N/A') AS WarehouseLocationCode

FROM dbo.SMRBIInventDimStaging IDS
LEFT JOIN dbo.SMRBIInventWarehouseStaging IWS
ON IWS.WarehouseId = IDS.InventLocationId

AND IWS.DataAreaId = IDS.DataAreaId
LEFT JOIN dbo.SMRBIInventSiteStaging ISS

ON ISS.SiteId = IDS.InventSiteId
AND ISS.DataAreaId = IDS.DataAreaId
;
