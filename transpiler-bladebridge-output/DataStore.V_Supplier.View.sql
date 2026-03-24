/****** Object:  View [DataStore].[V_Supplier]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_Supplier` AS


SELECT	VVS.VendTableRecId AS SupplierId
	  , UPPER(VVS.DataAreaId) AS CompanyCode
	  , UPPER(VVS.VendorAccountNumber) AS SupplierCode
	  , COALESCE(VVS.Name, '_N/A') AS SupplierName
	  , UPPER(VVS.VendorAccountNumber) || ' - ' || COALESCE(VVS.Name, '_N/A') AS SupplierCodeName
	  , COALESCE(CASE WHEN VVS.VendorGroupId = '' THEN NULL ELSE VVS.VendorGroupId END, '_N/A') AS SupplierGroupCode
	  , COALESCE((CASE WHEN VVGS.`Description` = '' THEN NULL ELSE VVGS.`Description` END), '_N/A') AS SupplierGroupName
	  , COALESCE(CASE WHEN VVS.VendorGroupId = '' THEN NULL ELSE VVS.VendorGroupId END, '_N/A') || ' - ' || COALESCE((CASE WHEN VVGS.`Description` = '' THEN NULL ELSE VVGS.`Description` END), '_N/A') AS SupplierGroupCodeName
	  , CAST(COALESCE(CASE WHEN VVS.FormattedPrimaryAddress = '' THEN NULL ELSE VVS.FormattedPrimaryAddress END, '_N/A') AS STRING) AS `Address` --Remove cast !!! 
	  , COALESCE(CASE WHEN VVS.AddressZipCode = '' THEN NULL ELSE VVS.AddressZipCode END, '_N/A') AS PostalCode
	  , COALESCE(CASE WHEN VVS.AddressCity = '' THEN NULL ELSE VVS.AddressCity END, '_N/A') AS City
	  , COALESCE(CASE WHEN VVS.AddressCountryRegionId = '' THEN NULL ELSE VVS.AddressCountryRegionId END, '_N/A') AS CountryRegionCode
	  , COALESCE(NULLIF(VVS.CompanyChainName, ''), N'_N/A') AS CompanyChainName

FROM dbo.SMRBIVendorStaging VVS

LEFT JOIN dbo.SMRBIVendVendorGroupStaging VVGS
ON VVS.VendorGroupId = VVGS.VendorGroupId
AND VVS.DataAreaId = VVGS.DataAreaId
;
