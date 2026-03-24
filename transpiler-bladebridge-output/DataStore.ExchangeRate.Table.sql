/****** Object:  Table [DataStore].[ExchangeRate]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ExchangeRate`(
	`ExchangeRateTypeCode`  STRING NOT NULL,
	`ExchangeRateTypeName`  STRING NOT NULL,
	`DataSource`  STRING NOT NULL,
	`FromCurrencyCode`  STRING NOT NULL,
	`ToCurrencyCode`  STRING NOT NULL,
	`ValidFrom` TIMESTAMP ,
	`ValidTo` TIMESTAMP ,
	`ExchangeRate`  DECIMAL(38,17) 
)
;
