/****** Object:  View [DWH].[V_DimEndCustomer]    Script Date: 03/03/2026 16:26:08 ******/















CREATE OR REPLACE VIEW `DWH`.`V_DimEndCustomer` AS         


SELECT    EndCustomerId
		, UPPER(EndCustomerCode) AS EndCustomerCode
		, EndCustomerName
		, EndCustomerCodeName
		, DimensionName     
		, COALESCE(C.SalesSegmentCode, '_N/A') AS SalesSegmentCode-- Industry Vertical in cube
		, COALESCE(C.SalesSubSegmentCode, '_N/A') AS SalesSubSegmentCode -- Industry Classification in cube
		, COALESCE(C.CompanyChain, '_N/A') AS CompanyChain
		, COALESCE(CO.CompanyCode, '_N/A') AS CompanyCode
		, COALESCE(TaxGroup, '_N/A') AS TaxGroup

FROM DataStore.EndCustomer EC -- Multiplication is desired, to get Endcustomer per CompanyCode

CROSS JOIN DataStore.Company CO

LEFT JOIN DataStore.Customer C
ON EC.EndCustomerCode = C.CustomerCode
AND CO.CompanyCode = C.CompanyCode



/* Create Unknown Member */

UNION ALL 
        
SELECT	DISTINCT -1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, UPPER(CompanyCode) AS CompanyId
		, '_N/A'

FROM DataStore.Company


;
