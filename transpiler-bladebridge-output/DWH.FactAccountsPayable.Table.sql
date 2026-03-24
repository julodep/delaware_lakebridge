/****** Object:  Table [DWH].[FactAccountsPayable]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactAccountsPayable`(
	`FactAccountsPayableId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimPurchaseInvoiceId` int NOT NULL,
	`DimIsOpenAmountId` `BOOLEAN` NOT NULL,
	`DimOutstandingPeriodId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimReportDateId` int NOT NULL,
	`DimDueDateId` int NOT NULL,
	`ReportMonthId` INT,
	`InvoiceDate` TIMESTAMP NOT NULL,
	`LastPaymentDate` TIMESTAMP NOT NULL,
	`DocumentDate` TIMESTAMP NOT NULL,
	`RecId` bigint NOT NULL,
	`PayablesVoucher`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
	`InvoiceAmountTC`  DECIMAL(38,6) ,
	`InvoiceAmountAC`  DECIMAL(38,6) ,
	`InvoiceAmountRC`  DECIMAL(38,6) ,
	`InvoiceAmountGC`  DECIMAL(38,6) ,
	`InvoiceAmountAC_Budget`  DECIMAL(38,6) ,
	`InvoiceAmountRC_Budget`  DECIMAL(38,6) ,
	`InvoiceAmountGC_Budget`  DECIMAL(38,6) ,
	`PaidAmountTC`  DECIMAL(38,6) ,
	`PaidAmountAC`  DECIMAL(38,6) ,
	`PaidAmountRC`  DECIMAL(38,6) ,
	`PaidAmountGC`  DECIMAL(38,6) ,
	`PaidAmountAC_Budget`  DECIMAL(38,6) ,
	`PaidAmountRC_Budget`  DECIMAL(38,6) ,
	`PaidAmountGC_Budget`  DECIMAL(38,6) ,
	`OpenAmountTC`  DECIMAL(38,6) ,
	`OpenAmountAC`  DECIMAL(38,6) ,
	`OpenAmountRC`  DECIMAL(38,6) ,
	`OpenAmountGC`  DECIMAL(38,6) ,
	`OpenAmountAC_Budget`  DECIMAL(38,6) ,
	`OpenAmountRC_Budget`  DECIMAL(38,6) ,
	`OpenAmountGC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateTC`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC`  DECIMAL(38,6) ,
	`AppliedExchangeRateRC`  DECIMAL(38,6) ,
	`AppliedExchangeRateGC`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`DimVoucherId` INT,
 CONSTRAINT `PK_FactAPCurrent` PRIMARY KEY CLUSTERED 
(
	`FactAccountsPayableId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK__FactAccou__DimDu__377BF4A1` FOREIGN KEY(`DimDueDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactAccou__DimRe__3B4C8585` FOREIGN KEY(`DimReportDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactAPCurrent_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactAPCurrent_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactAPCurrent_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactAPCurrent_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAPCurrent_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAPCurrent_DimIsOpenAmount` FOREIGN KEY(`DimIsOpenAmountId`)
REFERENCES `DWH`.`DimIsOpenAmount` (`DimIsOpenAmountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAPCurrent_DimOutstandingPeriod` FOREIGN KEY(`DimOutstandingPeriodId`)
REFERENCES `DWH`.`DimOutstandingPeriod` (`DimOutstandingPeriodId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAPCurrent_DimPurchaseInvoice` FOREIGN KEY(`DimPurchaseInvoiceId`)
REFERENCES `DWH`.`DimPurchaseInvoice` (`DimPurchaseInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAPCurrent_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
