/****** Object:  View [DataStore].[V_UnitOfMeasure]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore`.`V_UnitOfMeasure` AS 


SET (Denominator,DataFlow,Factor,InnerOffset,OuterOffset,Product,ItemNumber,Rounding,FromUOM,ToUOM,CompanyId,Denominator,Denominator,DataFlow,Factor,InnerOffset,OuterOffset,Product,ItemNumber,Rounding,FromUOM,ToUOM,CompanyId,Denominator,DataFlow,Factor,InnerOffset,OuterOffset,Product,ItemNumber,Rounding,FromUOM,ToUOM,CompanyId,Denominator,Denominator,Factor,InnerOffset,OuterOffset,Product,ItemNumber,Rounding,FromUOM,ToUOM,CompanyId) = (
WITH RequiredUnitConversions AS (

--Step 1a: Determine the required UOM conversion by looking at the sales orders, invoices and packing slips (in both directions). This level is added in case no standard out of the box conversion is available in D365
--Step 1b: Determine the required UOM conversions that are not linked to a product
--Step 2: Determine the ones that do not exist in D365
--Step 3: Do the actual conversion by converting first to the sales unit of the product, and then back to the required UOM

SELECT D.*
		, Factor = COALESCE(CONV.Factor, CONV3.Factor) / COALESCE(CONV2.Factor, CONV4.Factor)
FROM (

	SELECT C.* FROM

			(SELECT DISTINCT C.CompanyId
					, A.ItemNumber
					, A.FromUOM
					, B.ToUOM
			FROM
				(SELECT DISTINCT CompanyId = DataAreaId
						, ItemNumber = ItemNumber
						, FromUOM = UPPER(SalesUnitSymbol)
					FROM dbo.SMRBISalesOrderLineStaging
		
				UNION ALL
	 
				 SELECT DISTINCT CompanyId = DataAreaId
						, ItemNumber = ItemId
						, FromUOM = UPPER(SalesUnit)
					FROM dbo.SMRBICustInvoiceTransStaging
				
				UNION ALL
			
				 SELECT DISTINCT CompanyId = DataAreaId
						, ItemNumber = ItemId
						, FromUOM = UPPER(SalesUnit)
					FROM dbo.SMRBICustPackingSlipTransStaging) A
		
			CROSS JOIN (SELECT ToUOM = 'PCS' 
							UNION ALL SELECT ToUOM = 'BOX'
							UNION ALL SELECT ToUOM = 'PAL') B
				--> Determine the UOM need to convert to

			CROSS JOIN dbo.SMRBIOfficeAddinLegalEntityStaging C

			WHERE 1=1
				and NULLIF(ItemNumber, '') is not NULL
				and FromUOM != ToUOM
			
			UNION ALL
			
			SELECT DISTINCT C.CompanyId
					, A.ItemNumber
					, B.FromUOM
					, A.ToUOM
			FROM
				(SELECT DISTINCT CompanyId = DataAreaId
						, ItemNumber = ItemNumber
						, ToUOM = UPPER(SalesUnitSymbol)
					FROM dbo.SMRBISalesOrderLineStaging
		
				UNION ALL
	 
				 SELECT DISTINCT CompanyId = DataAreaId
						, ItemNumber = ItemId
						, ToUOM = UPPER(SalesUnit)
					FROM dbo.SMRBICustInvoiceTransStaging
				
				UNION ALL
			
				 SELECT DISTINCT CompanyId = DataAreaId
						, ItemNumber = ItemId
						, ToUOM = UPPER(SalesUnit)
					FROM dbo.SMRBICustPackingSlipTransStaging) A
		
			CROSS JOIN (SELECT FromUOM = 'PCS' 
							UNION ALL SELECT FromUOM = 'BOX'
							UNION ALL SELECT FromUOM = 'PAL') B
				--> Determine the UOM need to convert to

			CROSS JOIN dbo.SMRBIOfficeAddinLegalEntityStaging C

			WHERE 1=1
				and NULLIF(ItemNumber, '') is not NULL
				and FromUOM != ToUOM	
			
			) C

	LEFT JOIN dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging CONV
	ON CONV.ProductNumber = C.ItemNumber
		and CONV.FromUnitSymbol = C.FromUOM
		and CONV.ToUnitSymbol = C.ToUOM

	LEFT JOIN dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging CONV2
	ON CONV2.ProductNumber = C.ItemNumber
		and CONV2.FromUnitSymbol = C.ToUOM
		and CONV2.ToUnitSymbol = C.FromUOM

	WHERE 1=1
		and COALESCE(CONV.Factor, CONV2.Factor) is NULL) D

LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS
ON ERRPS.DataAreaId = D.CompanyId
	and ERRPS.ItemNumber = D.ItemNumber

LEFT JOIN dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging CONV
ON CONV.FromUnitSymbol = D.FromUOM
	and CONV.ToUnitSymbol = ERRPS.SalesUnitSymbol
	and CONV.ProductNumber = D.ItemNumber

LEFT JOIN dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging CONV2
ON CONV2.FromUnitSymbol = D.ToUOM
	and CONV2.ToUnitSymbol = ERRPS.SalesUnitSymbol
	and CONV2.ProductNumber = D.ItemNumber

LEFT JOIN 
	(SELECT ProductNumber, ToUnitSymbol FROM (
		SELECT RankNr = ROW_NUMBER() OVER (PARTITION BY ProductNumber ORDER BY Nbr DESC, CASE WHEN ToUnitSymbol = 'SU' THEN 1 ELSE '99' END), ProductNumber, ToUnitSymbol
		FROM (SELECT ProductNumber, ToUnitSymbol, Nbr = COUNT(*) FROM dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging GROUP BY ProductNumber, ToUnitSymbol) A) A
		--ORDER BY ProductNumber, ROW_NUMBER() OVER (PARTITION BY ProductNumber ORDER BY Nbr DESC, CASE WHEN ToUnitSymbol = 'SU' THEN 1 ELSE '99' END)
		GROUP BY RankNr, ProductNumber, ToUnitSymbol
		HAVING RankNr = 1) TMP
ON ERRPS.ItemNumber = TMP.ProductNumber

LEFT JOIN dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging CONV3
ON CONV3.FromUnitSymbol = D.FromUOM
	and CONV3.ToUnitSymbol = TMP.ToUnitSymbol
	and CONV3.ProductNumber = D.ItemNumber

LEFT JOIN dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging CONV4
ON CONV4.FromUnitSymbol = D.ToUOM
	and CONV4.ToUnitSymbol = TMP.ToUnitSymbol
	and CONV4.ProductNumber = D.ItemNumber

WHERE 1=1
	and COALESCE(CONV.Factor, CONV3.Factor) / COALESCE(CONV2.Factor, CONV4.Factor) is NOT NULL

)
SELECT	DISTINCT A.Denominator --Distinct is added to exclude any duplicates (if applicable)
		, A.DataFlow
		, A.Factor
		, A.InnerOffset
		, A.OuterOffset
		, A.Product
		, A.ItemNumber
		, A.Rounding
		, A.FromUOM
		, A.ToUOM
		, A.CompanyId AS CompanyCode

FROM (

SELECT	UOMC.Denominator
		, CAST('Normal' AS STRING)
		, CAST(UOMC.Factor AS DECIMAL(32,17))
		, UOMC.InnerOffset
		, UOMC.OuterOffset
		, UOMC.ProductNumber
		, COALESCE(ERRPS.ItemNumber, '_N/A')
		, UOMC.Rounding
		, UPPER(UOMC.FromUnitSymbol)
		, UPPER(UOMC.ToUnitSymbol)
		, COALESCE(ERRPS.DataAreaId, '_N/A')

FROM dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging UOMC

LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS
ON ERRPS.UOMC.ProductNumber
--Remark! there can exist a 1-many relationship between the product number and the item number. As such, both Item and Product have to be available

UNION ALL

SELECT	UOMC.Denominator
		, CAST('Normal|Non-Item' AS STRING)
		, CAST(UOMC.Factor AS DECIMAL(32,17))
		, UOMC.InnerOffset
		, UOMC.OuterOffset
		, 'NON-ITEM DRIVEN'
		, 'NON-ITEM DRIVEN'
		, UOMC.Rounding
		, UPPER(UOMC.FromUnitSymbol)
		, UPPER(UOMC.ToUnitSymbol)
		, COALESCE(OALES.CompanyId, '_N/A')

FROM dbo.SMRBIUnitOfMeasureConversionStaging UOMC
CROSS JOIN dbo.SMRBIOfficeAddinLegalEntityStaging OALES

UNION ALL

/* Create reverse conversion measures */
--Note : Normally 'Denominator' is used to calculate the reverse measures. However, in all cases, the denominator is 1 and the measures are duplicated with reverse factors.

SELECT	UOMC.Denominator
		, CAST('Normal (Rev)' AS STRING)
		, COALESCE(1 / NULLIF(UOMC.Factor,0), 0)
		, COALESCE(1 / NULLIF(UOMC.InnerOffset,0), 0)
		, COALESCE(1 / NULLIF(UOMC.OuterOffset,0), 0)
		, UOMC.ProductNumber
		, COALESCE(ERRPS.ItemNumber, '_N/A')
		, UOMC.Rounding
		, UPPER(UOMC.ToUnitSymbol)
		, UPPER(UOMC.FromUnitSymbol)
		, COALESCE(ERRPS.DataAreaId, '_N/A')

FROM dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging UOMC

LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS
ON ERRPS.UOMC.ProductNumber

LEFT JOIN 
	(SELECT	UOMC.Denominator
		, CAST(UOMC.Factor AS DECIMAL(32,17))
		, UOMC.InnerOffset
		, UOMC.OuterOffset
		, UOMC.ProductNumber
		, COALESCE(ERRPS.ItemNumber, '_N/A')
		, UOMC.Rounding
		, UPPER(UOMC.FromUnitSymbol)
		, UPPER(UOMC.ToUnitSymbol)
		, COALESCE(ERRPS.DataAreaId, '_N/A')
	FROM dbo.SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging UOMC
	LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS
	ON ERRPS.ProductNumber = UOMC.ProductNumber) UOM
ON UOM.Product = UOMC.ProductNumber
	and UOM.ItemNumber = COALESCE(ERRPS.ItemNumber, '_N/A')
	and UOM.FromUOM = UOMC.ToUnitSymbol
	and UOM.ToUOM = UOMC.FromUnitSymbol
	and UOM.CompanyId = COALESCE(ERRPS.DataAreaId, '_N/A')


WHERE 1=1
	and UOM.Product is NULL --Only create a reversed measure if it does not already exist in the standard table

UNION ALL

SELECT	Denominator = UOMC.Denominator
		, DataFlow = CAST('Normal|Non-Item' AS STRING)
		, Factor = COALESCE(1 / NULLIF(UOMC.Factor,0), 0)
		, InnerOffset = UOMC.InnerOffset
		, OuterOffset = UOMC.OuterOffset
		, Product = 'NON-ITEM DRIVEN'
		, ItemNumber = 'NON-ITEM DRIVEN'
		, Rounding = UOMC.Rounding
		, FromUOM = UPPER(UOMC.ToUnitSymbol)
		, ToUOM = UPPER(UOMC.FromUnitSymbol)
		, CompanyId = COALESCE(OALES.CompanyId, '_N/A')

FROM dbo.SMRBIUnitOfMeasureConversionStaging UOMC
CROSS JOIN dbo.SMRBIOfficeAddinLegalEntityStaging OALES


/* Create missing UOM conversions (e.g. BOX > PAL) */
UNION ALL

SELECT	Denominator = 1
		, DataFlow = CAST('Manual' AS STRING)
		, Factor = CAST(RUC.Factor AS DECIMAL(32,17))
		, InnerOffset = 0
		, OuterOffset = 0
		, Product = RUC.ItemNumber
		, ItemNumber = RUC.ItemNumber
		, Rounding = 1
		, FromUOM = RUC.FromUOM
		, ToUOM = RUC.ToUOM
		, CompanyId = RUC.CompanyId
FROM RequiredUnitConversions RUC

/* Create unknown member */

UNION ALL

SELECT	-1
		, CAST('Dummy' As STRING)
		, -1
		, -1
		, -1
		, '_N/A'
		, '_N/A'
		, -1
		, '_N/A'
		--, -1
		, '_N/A'
		--, -1
		, UPPER(CompanyId)

FROM dbo.SMRBIOfficeAddinLegalEntityStaging 

) a
;
);
