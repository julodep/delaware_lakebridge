/****** Object:  View [DataStore].[V_DeliveryMode]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore`.`V_DeliveryMode` AS 


SELECT	  UPPER(DDMS.DataAreaId) AS CompanyCode
		, UPPER(DDMS.ModeCode) AS DeliveryModeCode
		, COALESCE(DDMS.ModeDescription, '_N/A') AS DeliveryModeName

FROM dbo.SMRBIDeliveryModeStaging DDMS
;
