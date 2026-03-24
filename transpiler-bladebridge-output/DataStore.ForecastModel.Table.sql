/****** Object:  Table [DataStore].[ForecastModel]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`ForecastModel`(
	`CompanyCode`  STRING,
	`ForecastModelCode`  STRING,
	`ForecastModelName`  STRING NOT NULL,
	`ForecastSubModelCode`  STRING NOT NULL
)
;
