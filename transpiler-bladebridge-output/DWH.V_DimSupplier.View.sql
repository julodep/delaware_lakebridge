/****** Object:  View [DWH].[V_DimSupplier]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimSupplier` AS


SELECT	  SupplierId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(SupplierCode) AS SupplierCode
		, SupplierName
		, SupplierCodeName
		, SupplierGroupCode AS SupplierGroupCode
		, SupplierGroupName
		, SupplierGroupCodeName
		, `Address`
		, PostalCode
		, City
		, CountryRegionCode AS CountryRegionCode

FROM DataStore.Supplier

/* Create unknown member */
UNION ALL

SELECT	DISTINCT -1
		       , UPPER(CompanyCode) AS CompanyCode
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
