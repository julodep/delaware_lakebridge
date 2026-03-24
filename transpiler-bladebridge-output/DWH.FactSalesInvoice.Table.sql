/****** Object:  Table [DWH].[FactSalesInvoice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactSalesInvoice`(
	`FactSalesInvoiceId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimSalesInvoiceId` int NOT NULL,
	`DimSalesOrderId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimOrderCustomerId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimPaymentTermsId` int NOT NULL,
	`DimDeliveryModeId` int NOT NULL,
	`DimDeliveryTermsId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimInvoiceDateId` int NOT NULL,
	`DimRequestedDeliveryDateId` int NOT NULL,
	`DimConfirmedDeliveryDateId` int NOT NULL,
	`DimGLAccountId` int NOT NULL,
	`DimIntercompanyId` int NOT NULL,
	`InvoiceMonthId` int NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`TaxWriteCode` int NOT NULL,
	`SalesOrderStatus`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`SalesInvoiceLineNumber`  STRING NOT NULL,
	`SalesInvoiceLineNumberCombination`  STRING NOT NULL,
	`SalesUnit`  STRING NOT NULL,
	`InvoicedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`InvoicedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`InvoicedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`SalesPricePerUnitTC`  DECIMAL(38,6) NOT NULL,
	`SalesPricePerUnitAC`  DECIMAL(38,6) NOT NULL,
	`SalesPricePerUnitRC`  DECIMAL(38,6) NOT NULL,
	`SalesPricePerUnitGC`  DECIMAL(38,6) NOT NULL,
	`SalesPricePerUnitAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SalesPricePerUnitRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SalesPricePerUnitGC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossSalesTC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesRC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesGC`  DECIMAL(38,6) NOT NULL,
	`GrossSalesAC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossSalesRC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossSalesGC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountTC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountAC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountRC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountGC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountTC`  DECIMAL(32,17) NOT NULL,
	`InvoicedSalesAmountAC`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountRC`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountGC`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountTC`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountAC`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountRC`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountGC`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetSalesAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) NOT NULL,
	`CostOfGoodsSoldTC`  DECIMAL(38,7) NOT NULL,
	`CostOfGoodsSoldAC`  DECIMAL(38,6) NOT NULL,
	`CostOfGoodsSoldRC`  DECIMAL(38,6) NOT NULL,
	`CostOfGoodsSoldGC`  DECIMAL(38,6) NOT NULL,
	`CostOfGoodsSoldAC_Budget`  DECIMAL(38,6) NOT NULL,
	`CostOfGoodsSoldRC_Budget`  DECIMAL(38,6) NOT NULL,
	`CostOfGoodsSoldGC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossMarginTC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginAC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginRC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginGC`  DECIMAL(38,6) NOT NULL,
	`GrossMarginAC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossMarginRC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossMarginGC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedSalesAmountInclTaxAC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountInclTaxGC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountInclTaxRC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountInclTaxTC`  DECIMAL(38,6) ,
	`RebateAmountCancelledAC`  DECIMAL(38,6) ,
	`RebateAmountCancelledAC_Budget`  DECIMAL(38,6) ,
	`RebateAmountCancelledGC`  DECIMAL(38,6) ,
	`RebateAmountCancelledGC_Budget`  DECIMAL(38,6) ,
	`RebateAmountCancelledRC`  DECIMAL(38,6) ,
	`RebateAmountCancelledRC_Budget`  DECIMAL(38,6) ,
	`RebateAmountCancelledTC`  DECIMAL(38,6) ,
	`RebateAmountCompletedAC`  DECIMAL(38,6) ,
	`RebateAmountCompletedAC_Budget`  DECIMAL(38,6) ,
	`RebateAmountCompletedGC`  DECIMAL(38,6) ,
	`RebateAmountCompletedGC_Budget`  DECIMAL(38,6) ,
	`RebateAmountCompletedRC`  DECIMAL(38,6) ,
	`RebateAmountCompletedRC_Budget`  DECIMAL(38,6) ,
	`RebateAmountCompletedTC`  DECIMAL(38,6) ,
	`RebateAmountMarkedAC`  DECIMAL(38,6) ,
	`RebateAmountMarkedAC_Budget`  DECIMAL(38,6) ,
	`RebateAmountMarkedGC`  DECIMAL(38,6) ,
	`RebateAmountMarkedGC_Budget`  DECIMAL(38,6) ,
	`RebateAmountMarkedRC`  DECIMAL(38,6) ,
	`RebateAmountMarkedRC_Budget`  DECIMAL(38,6) ,
	`RebateAmountMarkedTC`  DECIMAL(38,6) ,
	`RebateAmountOriginalAC`  DECIMAL(38,6) ,
	`RebateAmountOriginalAC_Budget`  DECIMAL(38,6) ,
	`RebateAmountOriginalGC`  DECIMAL(38,6) ,
	`RebateAmountOriginalGC_Budget`  DECIMAL(38,6) ,
	`RebateAmountOriginalRC`  DECIMAL(38,6) ,
	`RebateAmountOriginalRC_Budget`  DECIMAL(38,6) ,
	`RebateAmountOriginalTC`  DECIMAL(38,6) ,
	`RebateAmountVarianceAC`  DECIMAL(38,6) ,
	`RebateAmountVarianceAC_Budget`  DECIMAL(38,6) ,
	`RebateAmountVarianceGC`  DECIMAL(38,6) ,
	`RebateAmountVarianceGC_Budget`  DECIMAL(38,6) ,
	`RebateAmountVarianceRC`  DECIMAL(38,6) ,
	`RebateAmountVarianceRC_Budget`  DECIMAL(38,6) ,
	`RebateAmountVarianceTC`  DECIMAL(38,6) ,
	`SurchargeTotalAC`  DECIMAL(38,6) ,
	`SurchargeTotalAC_Budget`  DECIMAL(38,6) ,
	`SurchargeTotalGC`  DECIMAL(38,6) ,
	`SurchargeTotalGC_Budget`  DECIMAL(38,6) ,
	`SurchargeTotalRC`  DECIMAL(38,6) ,
	`SurchargeTotalRC_Budget`  DECIMAL(38,6) ,
	`SurchargeTotalTC`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`DimBusinessSegmentId` int NOT NULL,
	`DimDepartmentId` int NOT NULL,
	`DimEndCustomerId` int NOT NULL,
	`DimLocationId` int NOT NULL,
	`DimShipmentContractId` int NOT NULL,
	`DimLocalAccountId` int NOT NULL,
	`DimProductFDId` int NOT NULL,
 CONSTRAINT `PK_FactSalesInvoice` PRIMARY KEY CLUSTERED 
(
	`FactSalesInvoiceId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesInvoice_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesInvoice_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesInvoice_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesInvoice_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimBusinessSegment` FOREIGN KEY(`DimBusinessSegmentId`)
REFERENCES `DWH`.`DimBusinessSegment` (`DimBusinessSegmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimConfirmedDeliveryDateId` FOREIGN KEY(`DimConfirmedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimDeliveryMode` FOREIGN KEY(`DimDeliveryModeId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimDeliveryTerms` FOREIGN KEY(`DimDeliveryTermsId`)
REFERENCES `DWH`.`DimDeliveryTerms` (`DimDeliveryTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimDepartment` FOREIGN KEY(`DimDepartmentId`)
REFERENCES `DWH`.`DimDepartment` (`DimDepartmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimEndCustomer` FOREIGN KEY(`DimEndCustomerId`)
REFERENCES `DWH`.`DimEndCustomer` (`DimEndCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimGLAccount` FOREIGN KEY(`DimGLAccountId`)
REFERENCES `DWH`.`DimGLAccount` (`DimGLAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimIntercompany` FOREIGN KEY(`DimIntercompanyId`)
REFERENCES `DWH`.`DimIntercompany` (`DimIntercompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimInvoiceDateId` FOREIGN KEY(`DimInvoiceDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimLocalAccount` FOREIGN KEY(`DimLocalAccountId`)
REFERENCES `DWH`.`DimLocalAccount` (`DimLocalAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimLocation` FOREIGN KEY(`DimLocationId`)
REFERENCES `DWH`.`DimLocation` (`DimLocationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimOrderCustomer` FOREIGN KEY(`DimOrderCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimPaymentTerms` FOREIGN KEY(`DimPaymentTermsId`)
REFERENCES `DWH`.`DimPaymentTerms` (`DimPaymentTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimProductFD` FOREIGN KEY(`DimProductFDId`)
REFERENCES `DWH`.`DimProductFD` (`DimProductFDId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimRequestedDeliveryDateId` FOREIGN KEY(`DimRequestedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimSalesInvoice` FOREIGN KEY(`DimSalesInvoiceId`)
REFERENCES `DWH`.`DimSalesInvoice` (`DimSalesInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimSalesOrder` FOREIGN KEY(`DimSalesOrderId`)
REFERENCES `DWH`.`DimSalesOrder` (`DimSalesOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesInvoice_DimShipmentContract` FOREIGN KEY(`DimShipmentContractId`)
REFERENCES `DWH`.`DimShipmentContract` (`DimShipmentContractId`)
;
