/****** Object:  View [DWH].[V_DimShipmentContract]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimShipmentContract` AS         


SELECT    ShipmentContractId
		, UPPER(ShipmentContractCode) AS ShipmentContractCode
		, ShipmentContractName
		, ShipmentContractCodeName
		, DimensionName         

FROM DataStore.ShipmentContract

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
