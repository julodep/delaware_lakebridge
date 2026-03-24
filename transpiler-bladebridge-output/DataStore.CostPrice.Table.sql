/****** Object:  Table [DataStore].[CostPrice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`CostPrice`(
	`ItemNumber`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`UnitCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`Price`  DECIMAL(38,9) ,
	`StartValidityDate` TIMESTAMP NOT NULL,
	`EndValidityDate` TIMESTAMP 
)
;
