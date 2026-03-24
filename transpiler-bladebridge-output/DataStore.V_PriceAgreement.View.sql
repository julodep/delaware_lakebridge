/****** Object:  View [DataStore].[V_PriceAgreement]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_PriceAgreement` AS 


SELECT
	  COALESCE(NULLIF(AccountRelation, ''), '_N/A') AS VendorCode
	, ItemRelation AS ProductCode
	, DataAreaId AS CompanyCode
	, Amount
	, Currency
	, FromDate
	, ToDate
	, QuantityAmountFrom AS QtyFrom
	, QuantityAmountTo AS QtyTo
	, UnitId
	, PriceUnit

FROM dbo.SMRBIPriceDiscTableStaging P

WHERE 1=1
	and Module = 2 -- Only vendor transactions
;
