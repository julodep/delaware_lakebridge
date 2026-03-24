/****** Object:  View [DWH].[V_DimDeliveryMode]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimDeliveryMode` AS 


SELECT    UPPER(CompanyCode) AS CompanyCode
		, UPPER(DeliveryModeCode) DeliveryModeCode
		, DeliveryModeName

FROM DataStore.DeliveryMode

/* Create unknown member */

UNION ALL

SELECT	  UPPER(CompanyCode)
		, '_N/A'
		, '_N/A'
FROM DataStore.Company
;
