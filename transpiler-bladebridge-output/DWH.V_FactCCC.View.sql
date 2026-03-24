/****** Object:  View [DWH].[V_FactCCC]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DWH`.`V_FactCCC` AS


SET (TIMESTAMP,TIMESTAMP,TIMESTAMP,TIMESTAMP,1,1,TIMESTAMP,TIMESTAMP,TIMESTAMP,TIMESTAMP,1,1,1,1,1,1,1) = (
WITH 
UnknownSupplier AS (
SELECT DimSupplierId
	 , C.CompanyCode
	 , SupplierGroupName
FROM DWH.DimSupplier DS
JOIN DWH.DimCompany C
ON DS.CompanyCode = C.CompanyCode
WHERE SupplierCode = '_N/A'
),

UnknownCustomer AS (
SELECT DimCustomerId
	 , C.CompanyCode
	 , CustomerGroupName
FROM DWH.DimCustomer DC
JOIN DWH.DimCompany C
ON DC.CompanyCode = C.CompanyCode
WHERE CustomerCode = '_N/A'
),

UnknownProduct AS (
SELECT DimProductId
	 , C.CompanyCode	
	 , ProductGroupName
FROM DWH.DimProduct DP
JOIN DWH.DimCompany C
ON DP.CompanyCode = C.CompanyCode
WHERE ProductCode = '_N/A'
)
SELECT *
	 , CCC.DSO + CCC.DIO_Value - CCC.DPO AS CashConversionCycle
FROM (

	--Load DSO:
SELECT	DSO.DimCustomerId AS DimCustomerId
	  , US.DimSupplierId AS DimSupplierId
	  , UP.DimProductId AS DimProductId
	  , C.DimCompanyId
	  , D.DimDateId AS DimReportingDateId
	  , DSO.DSO_CountBack AS DSO
	  , 1 AS DSOCount
	  , 0 AS DPO
	  , 0 AS DPOCount
	  , 0 AS DIO_Volume
	  , 0 AS DIO_Value
	  , 0 AS DIOCount
FROM TMP.DSO2 DSO
LEFT JOIN DWH.DimCompany C
ON DSO.C.DimCompanyId
LEFT JOIN UnknownSupplier US
ON C.US.CompanyCode
LEFT JOIN UnknownProduct UP
ON C.UP.CompanyCode
CROSS JOIN
	(SELECT D2.DimDateId
		FROM (SELECT DISTINCT LAST_DAY(TIMESTAMP) FROM DWH.DimDate) D1
		LEFT JOIN DWH.DimDate D2
		ON D1.D2.TIMESTAMP
		WHERE 1
		AND D2.YearId >= (SELECT YearId - 2 FROM DWH.DimDate WHERE TIMESTAMP IN (SELECT CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP))) --Go back MAX 2 years
		AND D2.TIMESTAMP <= current_timestamp()) D --Current month: there will be no balance yet!

UNION ALL

SELECT UC.DimCustomerId AS DimCustomerId
	 , DPO.DimSupplierId AS DimSupplierId
	 , UP.DimProductId AS DimProductId
	 , C.DimCompanyId
	 , D.DimDateId AS DimReportingDateId
	 , 0 AS DSO
	 , 0 AS DSOCount
	 , DPO.DPO_CountBack AS DPO
	 , 1 AS DPOCount
	 , 0 AS DIO_Volume
	 , 0 AS DIO_Value
	 , 0 AS DIOCount
FROM TMP.DPO2 DPO
LEFT JOIN DWH.DimCompany C
ON DPO.C.DimCompanyId
LEFT JOIN UnknownCustomer UC
ON C.UC.CompanyCode
LEFT JOIN UnknownProduct UP
ON C.UP.CompanyCode
CROSS JOIN
	(SELECT D2.DimDateId
	 FROM (SELECT DISTINCT LAST_DAY(TIMESTAMP) FROM DWH.DimDate) D1
	 LEFT JOIN DWH.DimDate D2
	 ON D1.D2.TIMESTAMP
	 WHERE 1
	 AND D2.YearId >= (SELECT YearId - 2 FROM DWH.DimDate WHERE TIMESTAMP IN (SELECT CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP))) --Go back MAX 2 years
	 AND D2.TIMESTAMP <= current_timestamp()) D --Current month: there will be no balance yet!

UNION ALL

SELECT UC.DimCustomerId AS DimCustomerId
	 , US.DimSupplierId AS DimSupplierId
	 , DIO.DimProductId AS DimProductId
	 , DIO.DimCompanyId AS DimCompanyId
	 , D.DimDateId AS DimReportingDateId
	 , 0 AS DSO
	 , 0 AS DSOCount
	 , 0 AS DPO
	 , 0 AS DPOCount
	 , DIO.DIO_Volume_CountBack AS DIO_Volume
	 , DIO.DIO_Value_CountBack AS DIO_Value
	 , 1 AS DIOCount
FROM TMP.DIO2 DIO
LEFT JOIN DWH.DimCompany C
ON DIO.C.DimCompanyId
LEFT JOIN UnknownCustomer UC
ON C.UC.CompanyCode
LEFT JOIN UnknownSupplier US
ON C.US.CompanyCode
CROSS JOIN
	(SELECT D2.DimDateId
		FROM (SELECT DISTINCT LAST_DAY(TIMESTAMP) AS TIMESTAMP FROM DWH.DimDate) D1
		LEFT JOIN DWH.DimDate D2
		ON D1.D2.TIMESTAMP
		WHERE 1
		AND D2.YearId >= (SELECT YearId - 2 FROM DWH.DimDate 
WHERE TIMESTAMP IN (SELECT CAST(CAST(current_timestamp() AS DATE) AS TIMESTAMP))) --Go back MAX 2 years
		AND D2.TIMESTAMP <= current_timestamp()) D --Current month: there will be no balance yet!
	) CCC
;
);
