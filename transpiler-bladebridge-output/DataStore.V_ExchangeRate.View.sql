/****** Object:  View [DataStore].[V_ExchangeRate]    Script Date: 03/03/2026 16:26:08 ******/
















CREATE OR REPLACE VIEW `DataStore`.`V_ExchangeRate` AS 


SELECT	DISTINCT ERES.RateTypeName AS ExchangeRateTypeCode
		       , ERES.RateTypeDescription AS ExchangeRateTypeName
			   , 'Dynamics365' AS DataSource
		       , ERES.FromCurrency AS FromCurrencyCode
		       , ERES.ToCurrency AS ToCurrencyCode
		       , ERES.StartDate AS ValidFrom
		       , ERES.EndDate AS ValidTo
		       , ERES.EXCHANGERATE/100 AS ExchangeRate
FROM dbo.SMRBIExchangeRateStaging ERES
CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) GC

-- Calculate the reverse exchange rate:

UNION ALL 

SELECT	DISTINCT ExchangRateTypeCode = ERES.RateTypeName
		    , ERES.RateTypeDescription AS ExchangeRateTypeName
		    , 'Dynamics365' AS DataSource
			, ERES.ToCurrency AS FromCurrencyCode
		    , ERES.FromCurrency ToCurrencyCode
		    , ERES.StartDate AS ValidFrom
		    , ERES.EndDate AS ValidTo
		    , 1/ ( ERES.EXCHANGERATE/100 ) AS ExchangeRate
FROM dbo.SMRBIExchangeRateStaging ERES
CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) GC

--Calculate ONE on ONE currencies:

UNION ALL

SELECT DISTINCT *
FROM (
	SELECT DISTINCT ERES.RateTypeName AS ExchangeRateTypeCode
		       , ERES.RateTypeDescription AS ExchangeRateTypeName
		       , 'Dynamics365' AS DataSource
			   , ERES.FromCurrency AS FromCurrencyCode
		       , ERES.FromCurrency AS ToCurrencyCode
		       , CAST('1900-01-01' AS TIMESTAMP) AS ValidFrom
		       , CAST('9999-12-31' AS TIMESTAMP) AS ValidTo
		       , 1 AS ExchangeRate

	FROM dbo.SMRBIExchangeRateStaging ERES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) GC

	UNION ALL 

	SELECT DISTINCT ERES.RateTypeName AS ExchangeRateTypeCode
		       , ERES.RateTypeDescription AS ExchangeRateTypeName
		       , 'Dynamics365' AS DataSource
			   , ERES.ToCurrency AS FromCurrencyCode
		       , ERES.ToCurrency AS ToCurrencyCode
		       , CAST('1900-01-01' AS TIMESTAMP) AS ValidFrom
		       , CAST('9999-12-31' AS TIMESTAMP) AS ValidTo
		       , 1 AS ExchangeRate

	FROM dbo.SMRBIExchangeRateStaging ERES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) GC

		) CUR
;
