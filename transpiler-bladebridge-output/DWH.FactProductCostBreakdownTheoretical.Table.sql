/****** Object:  Table [DWH].[FactProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactProductCostBreakdownTheoretical`(
	`FactProductCostBreakdownTheoreticalId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimFinishedProductId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimProductCostBreakdownHierarchyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimPriceCalculationDateId` int NOT NULL,
	`PriceType`  STRING NOT NULL,
	`CalculationType`  STRING,
	`CalculationCode`  STRING,
	`IsCalculatedPrice`  STRING NOT NULL,
	`IsMaxCalculation`  STRING NOT NULL,
	`IsMaxPrice`  STRING NOT NULL,
	`Levelling`  STRING NOT NULL,
	`IsActivePrice`  STRING NOT NULL,
	`Resource_`  STRING NOT NULL,
	`CostGroupCode`  STRING NOT NULL,
	`CostPriceVersion`  STRING NOT NULL,
	`CostPriceUnitSymbol`  STRING NOT NULL,
	`CostPriceModel`  STRING NOT NULL,
	`CostPriceCalculationNumber`  STRING NOT NULL,
	`CostPriceType`  STRING NOT NULL,
	`Level_1`  STRING NOT NULL,
	`Level_2`  STRING NOT NULL,
	`Level_3`  STRING NOT NULL,
	`ProductCostPriceAC`  DECIMAL(32,17) NOT NULL,
	`ProductCostPriceRC`  DECIMAL(31,17) NOT NULL,
	`ProductCostPriceGC`  DECIMAL(31,17) NOT NULL,
	`ProductCostPriceRC_Budget`  DECIMAL(31,17) NOT NULL,
	`ProductCostPriceGC_Budget`  DECIMAL(31,17) NOT NULL,
	`ComponentCostPriceAC`  DECIMAL(31,17) NOT NULL,
	`ComponentCostPriceRC`  DECIMAL(31,17) NOT NULL,
	`ComponentCostPriceGC`  DECIMAL(31,17) NOT NULL,
	`ComponentCostPriceRC_Budget`  DECIMAL(31,17) NOT NULL,
	`ComponentCostPriceGC_Budget`  DECIMAL(31,17) NOT NULL,
 CONSTRAINT `PK_FactProductCostBreakdownTheoretical` PRIMARY KEY CLUSTERED 
(
	`FactProductCostBreakdownTheoreticalId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductCostBreakdownTheoretical_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductCostBreakdownTheoretical_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductCostBreakdownTheoretical_DimProductCostBreakdownHierarchyId` FOREIGN KEY(`DimProductCostBreakdownHierarchyId`)
REFERENCES `DWH`.`DimProductCostBreakdownHierarchy` (`DimProductCostBreakdownHierarchyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductCostBreakdownTheoretical_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductCostBreakdownTheoretical_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductCostBreakdownTheoretical_DimFinishedProduct` FOREIGN KEY(`DimFinishedProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductCostBreakdownTheoretical_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductCostBreakdownTheoretical_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurch_DimPriceCalculationDateId` FOREIGN KEY(`DimPriceCalculationDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;
