/****** Object:  Table [DWH].[FactGeneralLedgerBudget]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactGeneralLedgerBudget`(
	`FactGeneralLedgerBudgetId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimBudgetModelId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimBudgetCodeId` int NOT NULL,
	`DimBudgetDateId` int NOT NULL,
	`DimGLAccountId` int NOT NULL,
	`DimIntercompanyId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`TransactionNumber`  STRING NOT NULL,
	`BudgetType`  STRING NOT NULL,
	`BudgetAmountTC`  DECIMAL(34,17) NOT NULL,
	`BudgetAmountAC`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountRC`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountGC`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,20) NOT NULL,
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
 CONSTRAINT `PK_FactGeneralLedgerBudget` PRIMARY KEY CLUSTERED 
(
	`FactGeneralLedgerBudgetId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK__FactGener__DimBu__5D378935` FOREIGN KEY(`DimBudgetDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactGener__DimBu__5E2BAD6E` FOREIGN KEY(`DimBudgetDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactGener__DimBu__7232A61B` FOREIGN KEY(`DimBudgetDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK__FactGener__DimBu__7326CA54` FOREIGN KEY(`DimBudgetDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedgerBudget_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedgerBudget_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedgerBudget_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactGeneralLedgerBudget_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimBudgetCode` FOREIGN KEY(`DimBudgetCodeId`)
REFERENCES `DWH`.`DimBudgetCode` (`DimBudgetCodeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimBudgetModel` FOREIGN KEY(`DimBudgetModelId`)
REFERENCES `DWH`.`DimBudgetModel` (`DimBudgetModelId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimBusinessSegment` FOREIGN KEY(`DimBusinessSegmentId`)
REFERENCES `DWH`.`DimBusinessSegment` (`DimBusinessSegmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimDepartment` FOREIGN KEY(`DimDepartmentId`)
REFERENCES `DWH`.`DimDepartment` (`DimDepartmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimEndCustomer` FOREIGN KEY(`DimEndCustomerId`)
REFERENCES `DWH`.`DimEndCustomer` (`DimEndCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimGLAccount` FOREIGN KEY(`DimGLAccountId`)
REFERENCES `DWH`.`DimGLAccount` (`DimGLAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimIntercompany` FOREIGN KEY(`DimIntercompanyId`)
REFERENCES `DWH`.`DimIntercompany` (`DimIntercompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimLocalAccount` FOREIGN KEY(`DimLocalAccountId`)
REFERENCES `DWH`.`DimLocalAccount` (`DimLocalAccountId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimLocation` FOREIGN KEY(`DimLocationId`)
REFERENCES `DWH`.`DimLocation` (`DimLocationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimProductFD` FOREIGN KEY(`DimProductFDId`)
REFERENCES `DWH`.`DimProductFD` (`DimProductFDId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactGeneralLedgerBudget_DimShipmentContract` FOREIGN KEY(`DimShipmentContractId`)
REFERENCES `DWH`.`DimShipmentContract` (`DimShipmentContractId`)
;
