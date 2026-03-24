/****** Object:  Table [DWH].[FactSalesBudget]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactSalesBudget`(
	`FactSalesBudgetId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimForecastModelId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimForecastDateId` int NOT NULL,
	`DimGLAccountId` int NOT NULL,
	`DimIntercompanyId` int NOT NULL,
	`ProductGroupCode`  STRING NOT NULL,
	`CustomerGroupCode`  STRING NOT NULL,
	`Comment`  STRING NOT NULL,
	`SalesUnit`  STRING NOT NULL,
	`ForecastQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`ForecastQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`ForecastQuantity_SalesUnit`  DECIMAL(38,6) ,
	`GrossSalesAmountTC`  DECIMAL(32,17) NOT NULL,
	`GrossSalesAmountAC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAmountRC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAmountGC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`CostPriceTC`  DECIMAL(38,7) NOT NULL,
	`CostPriceAC`  DECIMAL(38,6) NOT NULL,
	`CostPriceRC`  DECIMAL(38,6) NOT NULL,
	`CostPriceGC`  DECIMAL(38,6) NOT NULL,
	`CostPriceAC_Budget`  DECIMAL(38,6) NOT NULL,
	`CostPriceRC_Budget`  DECIMAL(38,6) NOT NULL,
	`CostPriceGC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossMarginTC`  DECIMAL(38,7) NOT NULL,
	`GrossMarginAC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginRC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginGC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginAC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossMarginRC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossMarginGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactSalesBudget` PRIMARY KEY CLUSTERED 
(
	`FactSalesBudgetId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK__FactSales__DimFo__70B471FD` FOREIGN KEY(`DimForecastDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesBudget_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesBudget_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimCurrency` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimCurrency1` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimForecastModel` FOREIGN KEY(`DimForecastModelId`)
REFERENCES `DWH`.`DimForecastModel` (`DimForeCastModelId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimGLAccount` FOREIGN KEY(`DimGLAccountId`)
REFERENCES `DWH`.`DimGLAccount` (`DimGLAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimIntercompany` FOREIGN KEY(`DimIntercompanyId`)
REFERENCES `DWH`.`DimIntercompany` (`DimIntercompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesBudget_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;
