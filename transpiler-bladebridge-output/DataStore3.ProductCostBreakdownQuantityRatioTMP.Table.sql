/****** Object:  Table [DataStore3].[ProductCostBreakdownQuantityRatioTMP]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore3`.`ProductCostBreakdownQuantityRatioTMP`(
	`DataAreaId`  STRING NOT NULL,
	`ItemNumber`  STRING,
	`PriceCalcId`  STRING NOT NULL,
	`ConsistOfPrice`  STRING NOT NULL,
	`QtyRatio` `FLOAT` ,
	`ConsumptionConstant`  DECIMAL(32,16) NOT NULL,
	`ConsumptionVariable`  DECIMAL(32,16) NOT NULL,
	`Qty`  DECIMAL(32,6) NOT NULL
)
;
