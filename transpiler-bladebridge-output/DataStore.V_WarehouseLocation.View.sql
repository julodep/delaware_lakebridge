/****** Object:  View [DataStore].[V_WarehouseLocation]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_WarehouseLocation` AS


SELECT	WWLS.WarehouseLocationRecId AS RecId
	  , UPPER(WWLS.DataAreaId) AS CompanyCode
	  , WWLS.WarehouseLocationId AS WarehouseLocationCode
	  , IWS.WarehouseId AS WarehouseCode
	  , COALESCE(IWS.WarehouseName, '_N/A') AS WarehouseName
	  , IWS.WarehouseId || ' - ' || COALESCE(IWS.WarehouseName, '_N/A') AS WarehouseCodeName
	  , COALESCE(NULLIF(WWLS.WareHouseLocationProfileId,''), '_N/A') AS WareHouseLocationType

FROM dbo.SMRBIWMSWarehouseLocationStaging WWLS

LEFT JOIN dbo.SMRBIInventWarehouseStaging IWS
	ON WWLS.WarehouseId = IWS.WarehouseId

AND WWLS.DataAreaId = IWS.DataAreaId
;
