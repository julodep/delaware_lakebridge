/****** Object:  Table [DataStore].[ProductCostBreakdownPrice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductCostBreakdownPrice`(
	`ItemNumber`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`UnitCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`PriceCalcId`  STRING NOT NULL,
	`Price`  DECIMAL(38,17) ,
	`VersionCode`  STRING NOT NULL,
	`PriceType`  STRING,
	`StartValidityDate` TIMESTAMP NOT NULL,
	`EndValidityDate` TIMESTAMP ,
	`CalculationNr`  STRING,
	`CalculationNrTech` BIGINT,
	`IsMaxCalculation`  STRING NOT NULL,
	`IsActivePrice`  STRING NOT NULL,
	`IsMaxPrice`  STRING NOT NULL
)
;
