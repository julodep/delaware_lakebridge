/****** Object:  View [DataStore].[V_Customer]    Script Date: 03/03/2026 16:26:08 ******/















CREATE OR REPLACE VIEW `DataStore`.`V_Customer` AS 


SELECT	  CCS.CustomerRecId AS CustomerId
		, UPPER(CCS.DataAreaId) AS CompanyCode
		, COALESCE(UPPER(CCS.CustomerAccount), '_N/A') AS CustomerCode
		, COALESCE(NULLIF(CCS.`Name`,''), '_N/A') AS CustomerName
		, UPPER(CCS.CustomerAccount) || '-' || COALESCE(CCS.`Name`, '_N/A') AS CustomerCodeName
		, COALESCE(NULLIF(CCS.CustomerGroupId,''), '_N/A') AS CustomerGroup
		, COALESCE(CCGES.NAME, '_N/A') AS CustomerGroupName
		, COALESCE(CASE WHEN CCS.CustomerGroupId = '' THEN '_N/A' ELSE CCS.CustomerGroupId END || ' - ' || COALESCE(CCGES.NAME, '_N/A'), '_N/A') AS CustomerGroupCodeName
		, COALESCE(NULLIF(CCS.CustClassificationId,''), '_N/A') AS CustomerClass
		, COALESCE(NULLIF(CPCGS.DESCRIPTION,''), '_N/A') AS CustomerClassName
		, COALESCE(CASE WHEN CCS.CustClassificationId = '' THEN '_N/A' ELSE CCS.CustClassificationId END || ' - ' || COALESCE(CPCGS.`Description`, '_N/A'), '_N/A') AS CustomerClassCodeName
		, CAST(COALESCE(NULLIF(CCS.Address,''), '_N/A') AS STRING) AS `Address`
		, COALESCE(NULLIF(CCS.ZipCode,''), '_N/A') AS PostalCode
		, COALESCE(NULLIF(CCS.City,''), '_N/A') AS City
		, COALESCE(NULLIF(TRANSL.CountryRegionId,''), '_N/A') AS Country
		, COALESCE(NULLIF(CCS.CommissionSalesGroupId,''), '_N/A') AS SalesGroup
		, COALESCE(NULLIF(CCS.CommissionSalesGroupId,''), '_N/A') AS Agent
		, COALESCE(HWS1.PersonnelNumber, '_N/A') AS SalesResponsibleCode
		, COALESCE(HWS1.`Name`, '_N/A') AS SalesResponsibleName
		, COALESCE(NULLIF(CCS.SalesSegmentId,''), '_N/A') AS SalesSegmentCode -- Industry Vertical in Cube
		, COALESCE(NULLIF(CCS.SalesSubSegmentId,''), '_N/A') AS SalesSubSegmentCode -- Industry Classification in Cube
		, COALESCE(NULLIF(CCS.DlvTerm, ''), '_N/A') AS DeliveryTerms
		, CAST(COALESCE(NULLIF(ESM.Name, ''), '_N/A') AS STRING) AS OnholdStatus
		, CAST(CASE WHEN CCS.CreditLimitIsMandatory = 0 THEN 'No' ELSE 'Yes' END AS STRING) AS CreditLimitIsMandatory
		, COALESCE(CCS.CreditLimit, 0) AS CreditLimit
		, COALESCE(NULLIF(CCS.YSLECompanyChainID, ''), N'_N/A') AS CompanyChain
		, COALESCE(NULLIF(CC.SALESTAXGROUP, ''), N'_N/A') AS TaxGroup

FROM dbo.SMRBICustomerStaging CCS

LEFT JOIN
	(SELECT DISTINCT CountryRegionId, ShortName 
		FROM dbo.SMRBILogisticsAddressCountryRegionTranslationStaging
		WHERE LanguageId = 'en-us' /* ADD/ALTER if required */
	) TRANSL
ON CCS.CountryRegionId = TRANSL.CountryRegionId

LEFT JOIN dbo.SMRBICustCustomerGroupStaging CCGES
ON CCS.CustomerGroupId = CCGES.CustGroup
	and CCS.DataAreaId = CCGES.DataAreaId

LEFT JOIN dbo.SMRBICustomerPriorityClassificationGroupStaging CPCGS
ON CCS.DataAreaId = CPCGS.DataAreaId
	and CCS.CustClassificationId = CPCGS.CustomerPriorityClassificationGroupCode

LEFT JOIN 
	(SELECT DISTINCT * FROM dbo.SMRBIHcmWorkerStaging) HWS1 --Data entity does not have a DataAreaId, but can be exported multiple times in different companies!
ON CCS.MainContactWorker = HWS1.HcmWorkerRecId

LEFT JOIN ETL.StringMap ESM
ON ESM.SourceTable = 'CustCustomerV2staging'
AND ESM.SourceColumn = 'OnHoldStatus'
AND ESM.Enum = CCS.OnHoldStatus

LEFT JOIN dbo.CustCustomerV3Staging CC
ON CCS.DATAAREAID = CC.DATAAREAID
AND CCS.CUSTOMERACCOUNT = CC.CUSTOMERACCOUNT

;
