/****** Object:  View [DWH].[V_DimProduct]    Script Date: 03/03/2026 16:26:09 ******/















CREATE OR REPLACE VIEW `DWH`.`V_DimProduct` AS 


SELECT	  ProductId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductCode) AS ProductCode
		, ProductName
		, ProductGroupCode
		, ProductGroupName
		, ProductGroupCodeName
		, ProductInventoryUnit
		, ProductPurchaseUnit
		, ProductSalesUnit
		, PhysicalUnitSymbol
		, PhysicalVolume
		, PhysicalWeight
		, PrimaryVendorCode
		, CountryOfOrigin
		, IntrastatCommodityCode
		, ABCClassification
		, Brand
		, Material
		, BusinessType

FROM DataStore.Product

/* Create unknown member */

UNION ALL

SELECT	DISTINCT -1
		, UPPER(CompanyCode)
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, 0
		, 0
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
FROM DataStore.Company

UNION ALL

-- ADD Cost Bill as product

SELECT	DISTINCT -2
		, UPPER(CompanyCode)
		, 'COST BILL'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, 0
		, 0
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
FROM DataStore.Company
;
