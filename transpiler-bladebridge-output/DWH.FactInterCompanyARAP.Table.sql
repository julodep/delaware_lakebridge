/****** Object:  Table [DWH].[FactInterCompanyARAP]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactInterCompanyARAP`(
	`FactInterCompanyARAPId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimSalesInvoiceId` int NOT NULL,
	`DimPurchaseInvoiceId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`CustomerSettlementCode`  STRING NOT NULL,
	`SupplierSettlement`  STRING NOT NULL,
	`DimCompanyID` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`InvoiceAmountTC`  DECIMAL(38,8) ,
	`InvoiceAmountAC`  DECIMAL(38,8) ,
	`InvoiceAmountRC`  DECIMAL(38,8) ,
	`InvoiceAmountGC`  DECIMAL(38,8) ,
	`OpenAmountTC`  DECIMAL(38,8) ,
	`OpenAmountAC`  DECIMAL(38,8) ,
	`OpenAmountRC`  DECIMAL(38,8) ,
	`OpenAmountGC`  DECIMAL(38,8) ,
	`AmountInvoiceTC`  DECIMAL(38,8) ,
	`AmountInvoiceAC`  DECIMAL(38,8) ,
	`AmountInvoiceRC`  DECIMAL(38,8) ,
	`AmountInvoiceGC`  DECIMAL(38,8) ,
	`DimInvoiceDateId` int NOT NULL,
	`DimDueDateId` int NOT NULL,
	`PostedDate` TIMESTAMP NOT NULL,
	`ETA` TIMESTAMP NOT NULL,
	`ETD` TIMESTAMP NOT NULL,
	`Branch`  STRING NOT NULL,
	`Departement`  STRING NOT NULL,
	`BusinessType`  STRING NOT NULL,
	`AR_AP_Type`  STRING NOT NULL,
	`Type`  STRING NOT NULL,
	`JobInvoice`  STRING NOT NULL,
	`POD`  STRING NOT NULL,
	`House`  STRING NOT NULL,
	`ShipmentNumber`  STRING NOT NULL,
	`Master`  STRING NOT NULL,
	`OrigCountry`  STRING NOT NULL,
	`OrigCountryName`  STRING NOT NULL,
	`POL`  STRING NOT NULL,
	`DimReportDateId` INT,
 CONSTRAINT `PK_FactInterCompanyARAPId` PRIMARY KEY CLUSTERED 
(
	`FactInterCompanyARAPId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactInterCompanyARAP_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactInterCompanyARAP_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactInterCompanyARAP_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactInterCompanyARAP_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactIntercompanyARAP_DimCompany` FOREIGN KEY(`DimCompanyID`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactIntercompanyARAP_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactIntercompanyARAP_DimPurchaseInvoice` FOREIGN KEY(`DimPurchaseInvoiceId`)
REFERENCES `DWH`.`DimPurchaseInvoice` (`DimPurchaseInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactIntercompanyARAP_DimSalesInvoice` FOREIGN KEY(`DimSalesInvoiceId`)
REFERENCES `DWH`.`DimSalesInvoice` (`DimSalesInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactIntercompanyARAP_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
