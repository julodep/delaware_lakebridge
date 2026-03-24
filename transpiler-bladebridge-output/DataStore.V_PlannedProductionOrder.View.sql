/****** Object:  View [DataStore].[V_PlannedProductionOrder]    Script Date: 03/03/2026 16:26:09 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_PlannedProductionOrder` AS


SELECT  ReqPO.RefId AS PlannedProductionOrderCode,
		/* Dimensions */
		COALESCE(NULLIF(ReqPO.ItemId,''), '_N/A') AS ProductCode,
		COALESCE(NULLIF(UPPER(ReqPO.DataAreaId),''), '_N/A') AS CompanyCode,
		COALESCE(NULLIF(ReqPO.CovInventDimId,''), '_N/A') AS ProductConfigurationCode,
		COALESCE(NULLIF(ProdTable.ProdId,''), '_N/A') AS ProductionOrderCode,

		/* Dates */
		ReqPO.ReqDate AS RequirementDate,
		ReqPO.ReqDateDLV AS RequestedDate,
		ReqPO.ReqDateOrder AS OrderDate,
		DATEADD(DAY, COALESCE(ReqPO.LeadTime, 0), ReqPO.ReqDateOrder) AS DeliveryDate,

		/* Planned Purchase Order Details */
		CAST(COALESCE(NULLIF(StringMapReqPOStatus.Name,''), '_N/A') AS STRING) AS Status,	
		COALESCE(ReqPO.LeadTime, 0) AS LeadTime,

		/* Key Figures */
		COALESCE(NULLIF(InventTableModule.UnitId,''), '_N/A') AS InventoryUnit,
		ReqPO.QTY AS RequirementQuantity,
		COALESCE(NULLIF(ReqPO.PurchUNIT,''), '_N/A') AS PurchaseUnit,
		ReqPO.PurchQTY AS PurchaseQuantity-- Requirement Quantity is translated to the purchase quantity (simply unit conversion, not actual ordered quantity)

FROM dbo.SMRBIReqPOStaging as ReqPO

LEFT JOIN dbo.SMRBIInventTableModuleStaging as InventTableModule
	ON InventTableModule.ItemId = ReqPO.ItemId
	AND InventTableModule.DataAreaId = ReqPO.DataAreaId
	AND InventTableModule.ModuleType = '0'

LEFT JOIN dbo.SMRBIProdTableStaging as ProdTable
	ON ProdTable.COMPANY = ReqPO.COMPANY  --ReqPO.DataAreaId
	AND ProdTable.ReqPOId = ReqPO.RefId

--/*To get enum labels*/
LEFT JOIN ETL.StringMap StringMapReqPOStatus
	ON StringMapReqPOStatus.SourceTable = 'ReqPOStatus'
	AND StringMapReqPOStatus.Enum = Cast(ReqPO.ReqPOStatus as STRING)

-- Only take Planned Purchase Orders (= RefType = '33')
WHERE 1=1
	and ReqPO.RefType = '31'
;
