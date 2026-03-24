/****** Object:  Table [DataStore4].[ProductCostBreakdownQuantityRatio]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore4`.`ProductCostBreakdownQuantityRatio`(
	`DataAreaId`  STRING NOT NULL,
	`ItemNumber`  STRING,
	`CalculationNr`  DECIMAL(38,0) ,
	`PriceCalcId`  STRING NOT NULL,
	`T1_PriceCalcId`  STRING NOT NULL,
	`T2_PriceCalcId`  STRING,
	`T3_PriceCalcId`  STRING,
	`T4_PriceCalcId`  STRING,
	`T5_PriceCalcId`  STRING,
	`T6_PriceCalcId`  STRING,
	`QtyRatioP0` `FLOAT` NOT NULL,
	`QtyRatioP1` `FLOAT` NOT NULL,
	`QtyRatioP2` `FLOAT` NOT NULL,
	`QtyRatioP3` `FLOAT` NOT NULL,
	`QtyRatioP4` `FLOAT` NOT NULL,
	`QtyRatioP5` `FLOAT` NOT NULL,
	`TotalQtyRatio`  DECIMAL(38,17) 
)
;
