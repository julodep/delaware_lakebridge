/****** Object:  Table [DWH].[FactPurchaseInvoice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactPurchaseInvoice`(
	`FactPurchaseInvoiceId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimPurchaseInvoiceId` int NOT NULL,
	`DimPurchaseOrderId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimGLAccountId` int NOT NULL,
	`DimIntercompanyId` int NOT NULL,
	`DimPaymentTermsId` int NOT NULL,
	`DimDeliveryModeId` int NOT NULL,
	`DimDeliveryTermsId` int NOT NULL,
	`DimInvoiceDateId` int NOT NULL,
	`InvoiceMonthId` INT,
	`PurchaseInvoiceCode`  STRING NOT NULL,
	`PurchaseOrderStatus`  STRING NOT NULL,
	`PurchaseInvoiceLineNumber`  DECIMAL(32,17) NOT NULL,
	`InvoiceLineNumberCombination`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`TaxWriteCode` int NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`InvoicedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`InvoicedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`InvoicedQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitTC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitAC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitRC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitGC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitAC_Budget`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitRC_Budget`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitGC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseTC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseAC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseRC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseGC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseAC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseRC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseGC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountTC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountAC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountRC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountGC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountTC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountAC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountRC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountGC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountTC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountAC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountRC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountGC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountTC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountAC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountRC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountGC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) NOT NULL,
	`SurchargeTransportTC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportAC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportRC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportGC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportGC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalTC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalAC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalRC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalGC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalGC_Budget`  DECIMAL(38,6) NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`DimProductFDId` int NOT NULL,
	`DimBusinessSegmentId` int NOT NULL,
	`DimEndCustomerId` int NOT NULL,
	`DimLocationId` int NOT NULL,
	`DimShipmentContractId` int NOT NULL,
	`DimLocalAccountId` int NOT NULL,
	`DimDepartmentId` int NOT NULL,
 CONSTRAINT `PK_FactPurchaseInvoice` PRIMARY KEY CLUSTERED 
(
	`FactPurchaseInvoiceId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK__FactPurch__DimInvoiceDateId` FOREIGN KEY(`DimInvoiceDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseInvoice_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseInvoice_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseInvoice_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactPurchaseInvoice_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimBusinessSegment` FOREIGN KEY(`DimBusinessSegmentId`)
REFERENCES `DWH`.`DimBusinessSegment` (`DimBusinessSegmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimDeliveryMode` FOREIGN KEY(`DimDeliveryModeId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimDeliveryTerms` FOREIGN KEY(`DimDeliveryTermsId`)
REFERENCES `DWH`.`DimDeliveryTerms` (`DimDeliveryTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimDepartment` FOREIGN KEY(`DimDepartmentId`)
REFERENCES `DWH`.`DimDepartment` (`DimDepartmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimEndCustomer` FOREIGN KEY(`DimEndCustomerId`)
REFERENCES `DWH`.`DimEndCustomer` (`DimEndCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimGLAccount` FOREIGN KEY(`DimGLAccountId`)
REFERENCES `DWH`.`DimGLAccount` (`DimGLAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimIntercompany` FOREIGN KEY(`DimIntercompanyId`)
REFERENCES `DWH`.`DimIntercompany` (`DimIntercompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimLocalAccount` FOREIGN KEY(`DimLocalAccountId`)
REFERENCES `DWH`.`DimLocalAccount` (`DimLocalAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimLocation` FOREIGN KEY(`DimLocationId`)
REFERENCES `DWH`.`DimLocation` (`DimLocationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimPaymentTerms` FOREIGN KEY(`DimPaymentTermsId`)
REFERENCES `DWH`.`DimPaymentTerms` (`DimPaymentTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimProductFD` FOREIGN KEY(`DimProductFDId`)
REFERENCES `DWH`.`DimProductFD` (`DimProductFDId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimPurchaseInvoice` FOREIGN KEY(`DimPurchaseInvoiceId`)
REFERENCES `DWH`.`DimPurchaseInvoice` (`DimPurchaseInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimPurchaseOrder` FOREIGN KEY(`DimPurchaseOrderId`)
REFERENCES `DWH`.`DimPurchaseOrder` (`DimPurchaseOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimShipmentContract` FOREIGN KEY(`DimShipmentContractId`)
REFERENCES `DWH`.`DimShipmentContract` (`DimShipmentContractId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseInvoice_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
