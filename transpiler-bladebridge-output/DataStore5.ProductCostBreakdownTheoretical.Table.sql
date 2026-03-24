/****** Object:  Table [DataStore5].[ProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore5`.`ProductCostBreakdownTheoretical`(
	`BOMCalcTransRecId` BIGINT,
	`CompanyCode`  STRING NOT NULL,
	`FinishedProductCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`CalculationType`  STRING,
	`PriceType`  STRING NOT NULL,
	`CalculationCode`  STRING NOT NULL,
	`IsCalculatedPrice`  STRING,
	`IsMaxCalculation`  STRING,
	`IsMaxPrice`  STRING,
	`Levelling`  STRING,
	`Resource_`  STRING,
	`CostGroupCode`  STRING,
	`CostPriceUnitSymbol`  STRING NOT NULL,
	`CostPriceCalculationNumber`  STRING NOT NULL,
	`CostPriceType`  STRING,
	`CostPriceVersion`  STRING NOT NULL,
	`CostPriceModel`  STRING,
	`CalculationDate` TIMESTAMP NOT NULL,
	`IsActivePrice`  STRING,
	`ProductCostPrice`  DECIMAL(38,17) ,
	`ComponentCostPrice`  DECIMAL(38,17) ,
	`CostPriceQty`  DECIMAL(38,17) ,
	`TotalQtyRatio`  DECIMAL(38,17) ,
	`ExcludedItems`  STRING NOT NULL
)
;
