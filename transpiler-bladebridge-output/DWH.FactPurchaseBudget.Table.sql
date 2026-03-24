/****** Object:  Table [DWH].[FactPurchaseBudget]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactPurchaseBudget`(
	`FactPurchaseBudgetId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimForecastModelId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimBudgetDateId` int NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`BudgetQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`BudgetQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`BudgetQuantity_SalesUnit`  DECIMAL(38,6) ,
	`PurchUnitPriceTC`  DECIMAL(38,6) ,
	`PurchUnitPriceAC`  DECIMAL(38,6) ,
	`PurchUnitPriceRC`  DECIMAL(38,6) ,
	`PurchUnitPriceGC`  DECIMAL(38,6) ,
	`PurchUnitPriceAC_Budget`  DECIMAL(38,6) ,
	`PurchUnitPriceRC_Budget`  DECIMAL(38,6) ,
	`PurchUnitPriceGC_Budget`  DECIMAL(38,6) ,
	`BudgetAmountTC`  DECIMAL(38,6) ,
	`BudgetAmountAC`  DECIMAL(38,6) ,
	`BudgetAmountRC`  DECIMAL(38,6) ,
	`BudgetAmountGC`  DECIMAL(38,6) ,
	`BudgetAmountAC_Budget`  DECIMAL(38,6) ,
	`BudgetAmountRC_Budget`  DECIMAL(38,6) ,
	`BudgetAmountGC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateTC`  DECIMAL(38,20) ,
	`AppliedExchangeRateRC`  DECIMAL(38,20) ,
	`AppliedExchangeRateAC`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC`  DECIMAL(38,20) ,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) ,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactPurchaseBudget` PRIMARY KEY CLUSTERED 
(
	`FactPurchaseBudgetId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseBudget_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseBudget_DimProductId` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseBudget_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseBudget_DimSupplierId` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurch_DimBudgetDateId` FOREIGN KEY(`DimBudgetDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurch_DimForecastModelId` FOREIGN KEY(`DimForecastModelId`)
REFERENCES `DWH`.`DimForecastModel` (`DimForeCastModelId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseBudget_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseBudget_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseBudget_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseBudget_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;
