/****** Object:  Table [DWH].[FactGeneralLedger]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactGeneralLedger`(
	`FactGeneralLedgerId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimGLAccountId` int NOT NULL,
	`DimIntercompanyId` int NOT NULL,
	`DimPostingDateId` int NOT NULL,
	`DocumentDate` TIMESTAMP NOT NULL,
	`RecId` bigint NOT NULL,
	`TransactionText`  STRING,
	`TransactionCode`  STRING NOT NULL,
	`AmountTC`  DECIMAL(32,17) NOT NULL,
	`AmountAC`  DECIMAL(32,17) NOT NULL,
	`AmountRC`  DECIMAL(32,17) NOT NULL,
	`AmountGC`  DECIMAL(38,6) NOT NULL,
	`AmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`AmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`AmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,17) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`DimBusinessSegmentId` int NOT NULL,
	`DimDepartmentId` int NOT NULL,
	`DimEndCustomerId` int NOT NULL,
	`DimLocationId` int NOT NULL,
	`DimShipmentContractId` int NOT NULL,
	`DimLocalAccountId` int NOT NULL,
	`DimProductFDId` int NOT NULL,
	`IsDebitCredit`  STRING,
	`DimVoucherId` INT,
	`DimSupplierId` INT,
	`Voucher`  STRING,
 CONSTRAINT `PK_FactGeneralLedger` PRIMARY KEY CLUSTERED 
(
	`FactGeneralLedgerId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK__DimPostingDateId` FOREIGN KEY(`DimPostingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedger_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedger_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedger_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedger_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimBusinessSegment` FOREIGN KEY(`DimBusinessSegmentId`)
REFERENCES `DWH`.`DimBusinessSegment` (`DimBusinessSegmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimDepartment` FOREIGN KEY(`DimDepartmentId`)
REFERENCES `DWH`.`DimDepartment` (`DimDepartmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimEndCustomer` FOREIGN KEY(`DimEndCustomerId`)
REFERENCES `DWH`.`DimEndCustomer` (`DimEndCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimGLAccount` FOREIGN KEY(`DimGLAccountId`)
REFERENCES `DWH`.`DimGLAccount` (`DimGLAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimIntercompany` FOREIGN KEY(`DimIntercompanyId`)
REFERENCES `DWH`.`DimIntercompany` (`DimIntercompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimLocalAccount` FOREIGN KEY(`DimLocalAccountId`)
REFERENCES `DWH`.`DimLocalAccount` (`DimLocalAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimLocation` FOREIGN KEY(`DimLocationId`)
REFERENCES `DWH`.`DimLocation` (`DimLocationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimProductFD` FOREIGN KEY(`DimProductFDId`)
REFERENCES `DWH`.`DimProductFD` (`DimProductFDId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedger_DimShipmentContract` FOREIGN KEY(`DimShipmentContractId`)
REFERENCES `DWH`.`DimShipmentContract` (`DimShipmentContractId`)
;
