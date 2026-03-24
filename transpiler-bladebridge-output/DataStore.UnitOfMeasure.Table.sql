/****** Object:  Table [DataStore].[UnitOfMeasure]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`UnitOfMeasure`(
	`Denominator` int NOT NULL,
	`DataFlow`  STRING,
	`Factor`  DECIMAL(38,21) ,
	`InnerOffset`  DECIMAL(38,12) NOT NULL,
	`OuterOffset`  DECIMAL(38,12) NOT NULL,
	`Product`  STRING NOT NULL,
	`ItemNumber`  STRING NOT NULL,
	`Rounding` int NOT NULL,
	`FromUOM`  STRING,
	`ToUOM`  STRING,
	`CompanyCode`  STRING
)
;
