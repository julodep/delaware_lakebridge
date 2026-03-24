/****** Object:  Table [DWH].[FactProductionTimeRegistration]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactProductionTimeRegistration`(
	`FactProductionTimeRegistrationId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimProductionOrderId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimResourceId` int NOT NULL,
	`DimRouteId` int NOT NULL,
	`DimOperationsId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimPostedJournalDateId` int NOT NULL,
	`OperatorType`  STRING NOT NULL,
	`OperatorName`  STRING NOT NULL,
	`Shift`  STRING NOT NULL,
	`MachineTimeMinutes`  DECIMAL(35,6) ,
	`MachineTimeHours`  DECIMAL(32,6) ,
	`MachineTimeDays`  DECIMAL(35,9) ,
	`MachineCostTC`  DECIMAL(32,6) ,
	`MachineCostAC`  DECIMAL(38,6) ,
	`MachineCostRC`  DECIMAL(38,6) ,
	`MachineCostGC`  DECIMAL(38,6) ,
	`MachineCostAC_Budget`  DECIMAL(38,6) ,
	`MachineCostRC_Budget`  DECIMAL(38,6) ,
	`MachineCostGC_Budget`  DECIMAL(38,6) ,
	`OperatorTimeMinutes`  DECIMAL(35,6) ,
	`OperatorTimeHours`  DECIMAL(32,6) ,
	`OperatorTimeDays`  DECIMAL(35,9) ,
	`LabourCostTC`  DECIMAL(38,6) ,
	`LabourCostAC`  DECIMAL(38,6) ,
	`LabourCostGC`  DECIMAL(38,6) ,
	`LabourCostRC`  DECIMAL(38,6) ,
	`LabourCostAC_Budget`  DECIMAL(38,6) ,
	`LabourCostGC_Budget`  DECIMAL(38,6) ,
	`LabourCostRC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateTC`  DECIMAL(38,6) ,
	`AppliedExchangeRateRC`  DECIMAL(38,20) ,
	`AppliedExchangeRateAC`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC`  DECIMAL(38,20) ,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) ,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactProductionTimeRegistration` PRIMARY KEY CLUSTERED 
(
	`FactProductionTimeRegistrationId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductionTimeRegistration_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductionTimeRegistration_DimOperationsId` FOREIGN KEY(`DimOperationsId`)
REFERENCES `DWH`.`DimOperations` (`DimOperationsId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductionTimeRegistration_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductionTimeRegistration_DimRouteId` FOREIGN KEY(`DimRouteId`)
REFERENCES `DWH`.`DimRoute` (`DimRouteId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimPostedJournalDateId` FOREIGN KEY(`DimPostedJournalDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimProductionOrderId` FOREIGN KEY(`DimProductionOrderId`)
REFERENCES `DWH`.`DimProductionOrder` (`DimProductionOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimResourceId` FOREIGN KEY(`DimResourceId`)
REFERENCES `DWH`.`DimResource` (`DimResourceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionTimeRegistration_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;
