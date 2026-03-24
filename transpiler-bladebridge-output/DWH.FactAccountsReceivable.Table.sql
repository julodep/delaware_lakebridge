/****** Object:  Table [DWH].[FactAccountsReceivable]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactAccountsReceivable`(
	`FactAccountsReceivableId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimSalesInvoiceId` int NOT NULL,
	`DimIsOpenAmountId` `BOOLEAN` NOT NULL,
	`DimOutstandingPeriodId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimReportDateId` int NOT NULL,
	`DimDueDateId` int NOT NULL,
	`ReportMonthId` int NOT NULL,
	`InvoiceDate` TIMESTAMP NOT NULL,
	`LastPaymentDate` TIMESTAMP NOT NULL,
	`DocumentDate` TIMESTAMP NOT NULL,
	`RecId` bigint NOT NULL,
	`ReceivablesVoucher`  STRING NOT NULL,
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
 CONSTRAINT `PK_FactARCurrent1` PRIMARY KEY CLUSTERED 
(
	`FactAccountsReceivableId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK__FactAccou__DimDu__3D34CDF7` FOREIGN KEY(`DimDueDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactAccou__DimIs__7EAD8B99` FOREIGN KEY(`DimIsOpenAmountId`)
REFERENCES `DWH`.`DimIsOpenAmount` (`DimIsOpenAmountId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactAccou__DimOu__7FA1AFD2` FOREIGN KEY(`DimOutstandingPeriodId`)
REFERENCES `DWH`.`DimOutstandingPeriod` (`DimOutstandingPeriodId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactAccou__DimRe__41055EDB` FOREIGN KEY(`DimReportDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactARCurrent_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactARCurrent_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactARCurrent_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactARCurrent_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAccountsReceivable_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactAccountsReceivable_DimSalesInvoice` FOREIGN KEY(`DimSalesInvoiceId`)
REFERENCES `DWH`.`DimSalesInvoice` (`DimSalesInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactARCurrent_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;
