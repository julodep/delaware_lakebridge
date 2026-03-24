/****** Object:  Table [DWH].[FactRFQ]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactRFQ`(
	`FactRFQId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimDeliveryModeId` int NOT NULL,
	`DimDeliveryTermsId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimDeliveryDateId` int NOT NULL,
	`DimExpiryDateId` int NOT NULL,
	`RFQCaseCode`  STRING NOT NULL,
	`RFQCaseName`  STRING NOT NULL,
	`RFQCaseLineNumber` bigint NOT NULL,
	`RFQCode`  STRING NOT NULL,
	`RFQName`  STRING NOT NULL,
	`RFQLineNumber`  DECIMAL(32,16) NOT NULL,
	`RFQCaseStatusLow`  STRING NOT NULL,
	`RFQCaseStatusHigh`  STRING NOT NULL,
	`RFQLineStatus`  STRING NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`PurchPriceTC`  DECIMAL(38,6) ,
	`PurchPriceAC`  DECIMAL(38,6) ,
	`PurchPriceRC`  DECIMAL(38,6) ,
	`PurchPriceGC`  DECIMAL(38,6) ,
	`PurchPriceAC_Budget`  DECIMAL(38,6) ,
	`PurchPriceRC_Budget`  DECIMAL(38,6) ,
	`PurchPriceGC_Budget`  DECIMAL(38,6) ,
	`RFQLineAmountTC`  DECIMAL(38,6) ,
	`RFQLineAmountAC`  DECIMAL(38,6) ,
	`RFQLineAmountRC`  DECIMAL(38,6) ,
	`RFQLineAmountGC`  DECIMAL(38,6) ,
	`RFQLineAmountAC_Budget`  DECIMAL(38,6) ,
	`RFQLineAmountRC_Budget`  DECIMAL(38,6) ,
	`RFQLineAmountGC_Budget`  DECIMAL(38,6) ,
	`CostAvoidanceAmountTC`  DECIMAL(38,6) ,
	`CostAvoidanceAmountAC`  DECIMAL(38,6) ,
	`CostAvoidanceAmountRC`  DECIMAL(38,6) ,
	`CostAvoidanceAmountGC`  DECIMAL(38,6) ,
	`CostAvoidanceAmountAC_Budget`  DECIMAL(38,6) ,
	`CostAvoidanceAmountRC_Budget`  DECIMAL(38,6) ,
	`CostAvoidanceAmountGC_Budget`  DECIMAL(38,6) ,
	`MaxPurchPriceTC`  DECIMAL(38,6) ,
	`MaxPurchPriceAC`  DECIMAL(38,6) ,
	`MaxPurchPriceRC`  DECIMAL(38,6) ,
	`MaxPurchPriceGC`  DECIMAL(38,6) ,
	`MaxPurchPriceAC_Budget`  DECIMAL(38,6) ,
	`MaxPurchPriceRC_Budget`  DECIMAL(38,6) ,
	`MaxPurchPriceGC_Budget`  DECIMAL(38,6) ,
	`MinPurchPriceTC`  DECIMAL(38,6) ,
	`MinPurchPriceAC`  DECIMAL(38,6) ,
	`MinPurchPriceRC`  DECIMAL(38,6) ,
	`MinPurchPriceGC`  DECIMAL(38,6) ,
	`MinPurchPriceAC_Budget`  DECIMAL(38,6) ,
	`MinPurchPriceRC_Budget`  DECIMAL(38,6) ,
	`MinPurchPriceGC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateTC`  DECIMAL(38,20) ,
	`AppliedExchangeRateRC`  DECIMAL(38,20) ,
	`AppliedExchangeRateAC`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC`  DECIMAL(38,20) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactRFQ` PRIMARY KEY CLUSTERED 
(
	`FactRFQId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactRFQ_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactRFQ_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactRFQ_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactRFQ_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimDeliveryDateId` FOREIGN KEY(`DimDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimDeliveryMode` FOREIGN KEY(`DimDeliveryModeId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimDeliveryTerms` FOREIGN KEY(`DimDeliveryTermsId`)
REFERENCES `DWH`.`DimDeliveryTerms` (`DimDeliveryTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimExpiryDateId` FOREIGN KEY(`DimExpiryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactRFQ_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
