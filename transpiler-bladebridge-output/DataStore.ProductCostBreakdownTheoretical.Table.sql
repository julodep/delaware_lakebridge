/****** Object:  Table [DataStore].[ProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductCostBreakdownTheoretical`(
	`BOM` int NOT NULL,
	`BOMCalcTransRecId` bigint NOT NULL,
	`CalcGroupCode`  STRING NOT NULL,
	`CalcType` int NOT NULL,
	`ConsistOfPrice`  STRING NOT NULL,
	`ConsumptionConstant`  DECIMAL(32,16) NOT NULL,
	`ConsumptionVariable`  DECIMAL(32,16) NOT NULL,
	`ConsumpType` int NOT NULL,
	`CostCalculationMethod` int NOT NULL,
	`CostGroupCode`  STRING NOT NULL,
	`CostMarkup`  DECIMAL(32,16) NOT NULL,
	`CostMarkupQty`  DECIMAL(32,16) NOT NULL,
	`CostPrice`  DECIMAL(32,16) NOT NULL,
	`CostPriceModelUsed` int NOT NULL,
	`CostPriceQty`  DECIMAL(32,16) NOT NULL,
	`CostPriceUnit`  DECIMAL(32,12) NOT NULL,
	`DataAreaId`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`Level_` int NOT NULL,
	`LineNum`  DECIMAL(32,16) NOT NULL,
	`NetWeightQty`  DECIMAL(32,12) NOT NULL,
	`NumOfSeries`  DECIMAL(32,6) NOT NULL,
	`OprId`  STRING NOT NULL,
	`OprNum` int NOT NULL,
	`OprNumNext` int NOT NULL,
	`OprPriority` int NOT NULL,
	`ParentBOMCalcTrans` bigint NOT NULL,
	`PriceCalcId`  STRING NOT NULL,
	`Qty`  DECIMAL(32,6) NOT NULL,
	`Resource_`  STRING NOT NULL,
	`SalesMarkup`  DECIMAL(32,6) NOT NULL,
	`SalesMarkupQty`  DECIMAL(32,6) NOT NULL,
	`SalesPrice`  DECIMAL(32,6) NOT NULL,
	`SalesPriceQty`  DECIMAL(32,6) NOT NULL,
	`SalesPriceUnit`  DECIMAL(32,12) NOT NULL,
	`TransDate` TIMESTAMP NOT NULL,
	`UnitCode`  STRING NOT NULL
)
;
