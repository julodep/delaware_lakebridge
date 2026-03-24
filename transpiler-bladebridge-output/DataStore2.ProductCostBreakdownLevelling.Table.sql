/****** Object:  Table [DataStore2].[ProductCostBreakdownLevelling]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.`ProductCostBreakdownLevelling`(
	`CompanyCode`  STRING,
	`ItemNumber`  STRING,
	`InventDimCode`  STRING,
	`StartValidityDate` TIMESTAMP ,
	`EndValidityDate` TIMESTAMP ,
	`CalculationNr`  DECIMAL(38,0) ,
	`LowestLevelCalc`  STRING NOT NULL,
	`PriceCalcId`  STRING,
	`Levelling`  STRING NOT NULL,
	`LevellingNr` int NOT NULL
)
;
