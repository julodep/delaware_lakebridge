/****** Object:  Table [DataStore2].[ExchangeRateExplosion]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore2`.`ExchangeRateExplosion`(
	`ExchangeRateTypeCode`  STRING NOT NULL,
	`ExchangeRateTypeName`  STRING NOT NULL,
	`DataSource`  STRING NOT NULL,
	`FromCurrencyCode`  STRING NOT NULL,
	`ToCurrencyCode`  STRING NOT NULL,
	`ExchangeRate`  DECIMAL(38,17) ,
	TIMESTAMP TIMESTAMP NOT NULL
)
;
