/****** Object:  View [DataStore2].[V_ExchangeRateExplosion]    Script Date: 03/03/2026 16:26:08 ******/





CREATE OR REPLACE VIEW `DataStore2`.`V_ExchangeRateExplosion` AS 

SELECT ExchangeRateTypeCode
		, ExchangeRateTypeName
		, DataSource
		, FromCurrencyCode
		, ToCurrencyCode
		, AVG(ExchangeRate) AS ExchangeRate
		, D.TIMESTAMP
FROM DataStore.ExchangeRate ER
INNER JOIN DataStore.Date D
ON D.TIMESTAMP BETWEEN ER.ValidFrom AND ER.ValidTo
GROUP BY ExchangeRateTypeCode
		, ExchangeRateTypeName
		, DataSource
		, FromCurrencyCode
		, ToCurrencyCode
		, D.TIMESTAMP
;
