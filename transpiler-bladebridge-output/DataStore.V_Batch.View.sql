/****** Object:  View [DataStore].[V_Batch]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_Batch` AS 


SELECT	IIBS.InventBatchRecId AS RecId
		, UPPER(IIBS.DataAreaId) AS CompanyCode
		, CAST(IIBS.BatchNumber AS STRING) AS BatchCode
		, COALESCE(UPPER(IIBS.ItemNumber), '_N/A') AS ProductCode
		, COALESCE(CASE WHEN IIBS.BatchDescription = '' THEN '_N/A' ELSE IIBS.BatchDescription END, '_N/A') AS `Description`
		, COALESCE(NULLIF(IIBS.BatchExpirationDate, ''), '1900-01-01') AS ExpiryDate
		, COALESCE(NULLIF(IIBS.ManufacturingDate, ''), '1900-01-01') AS ProductionDate

FROM dbo.SMRBIInventItemBatchStaging IIBS
;
