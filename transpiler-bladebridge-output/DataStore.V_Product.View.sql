/****** Object:  View [DataStore].[V_Product]    Script Date: 03/03/2026 16:26:08 ******/








/* Create dynamic table for product attributes */

CREATE OR REPLACE VIEW `DataStore`.`V_Product` AS


SELECT	ProductId
	  , CompanyId AS CompanyCode
	  , ProductCode
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
	  , PrimaryVendorId AS PrimaryVendorCode
	  , CountryOfOrigin
	  , IntrastatCommodityCode
	  , ABCClassification
--Product Attributes:
	  , Brand /* ADD IF REQUIRED */
	  , Material /* ADD IF REQUIRED */
	  , BusinessType /* ADD IF REQUIRED */

FROM
	(SELECT	ProductId
		  , CompanyId
		  , ProductCode
		  , ProductName
		  , ProductGroupCode
		  , ProductGroupName
		  , ProductGroupCodeName
		  , ProductInventoryUnit
		  , ProductPurchaseUnit
		  , PhysicalUnitSymbol
		  , PhysicalVolume
		  , PhysicalWeight
		  , ProductSalesUnit
		  , PrimaryVendorId
		  , CountryOfOrigin
		  , IntrastatCommodityCode
		  , ABCClassification
	--Product Attributes:
		  , COALESCE(NULLIF(MIN(P.`Brand`), ''), '_N/A') AS Brand--To be completed (field needs to be sourced) !!!
		  , COALESCE(NULLIF(MIN(P.`Material`), ''), '_N/A') AS Material --To be completed (field needs to be sourced) !!!
		  , COALESCE(NULLIF(MIN(P.`BusinessType`), ''), '_N/A') AS BusinessType --To be completed (field needs to be sourced) !!!
	FROM
		(SELECT	ERRPS.ProductRecId AS ProductId
				, UPPER(ERRPS.DataAreaId) AS CompanyId
				, COALESCE(NULLIF(UPPER(ERRPS.ItemNumber),''), '_N/A') AS ProductCode
				, COALESCE(NULLIF(ERPTS.ProductName, ''), '_N/A') AS ProductName
				, COALESCE(NULLIF(ERRPS.ProductGroupId, ''), '_N/A') AS ProductGroupCode
				, COALESCE(CASE WHEN IPGS.Name = '' THEN NULL ELSE IPGS.Name END, '_N/A') AS ProductGroupName
				, COALESCE((ERRPS.ProductGroupId || ' - '|| COALESCE(CASE WHEN IPGS.Name = '' THEN NULL ELSE IPGS.Name END, '_N/A')), '_N/A') AS ProductGroupCodeName
			  
				, COALESCE(NULLIF(UPPER(ERRPS.InventoryUnitSymbol), ''), '_N/A') AS ProductInventoryUnit
				, COALESCE(NULLIF(UPPER(ERRPS.PurchaseUnitSymbol), ''), '_N/A') AS ProductPurchaseUnit
				, COALESCE(NULLIF(UPPER(ERRPS.SalesUnitSymbol), ''), '_N/A') AS ProductSalesUnit
				, COALESCE(PDGD.PhysicalDepth, 0) * COALESCE(PDGD.PhysicalHeight, 0) * COALESCE(PDGD.PhysicalWidth, 0) AS PhysicalVolume
				, COALESCE(PDGD.PhysicalWeight, 0) AS PhysicalWeight
				, COALESCE(PDGD.PhysicalUnitSymbol, '_N/A') AS PhysicalUnitSymbol
			 
				, COALESCE(NULLIF(PPAVS.ApprovedVendorAccountNumber, ''), NULLIF(ERRPS.PrimaryVendorAccountNumber, ''), '_N/A') AS PrimaryVendorId

				, COALESCE(NULLIF(ERRPS.OriginCountryRegionId, ''), '_N/A') AS CountryOfOrigin
				, COALESCE(NULLIF(ERRPS.IntrastatCommodityCode, ''), '_N/A') AS IntrastatCommodityCode

				, ABCClassification = CAST(CASE WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), 0) = 0 THEN 'None'
							WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), 0) = 1 THEN 'A'
							WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), 0) = 2 THEN 'B'
							WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), 0) = 3 THEN 'C'
							END AS STRING)

				, PROD.AttributeName
				, PROD.AttributeValue

		FROM dbo.SMRBIEcoResReleasedProductStaging ERRPS

		--Required for Manufacturing Site
		LEFT JOIN 
			(
