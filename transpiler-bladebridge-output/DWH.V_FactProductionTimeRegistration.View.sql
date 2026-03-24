/****** Object:  View [DWH].[V_FactProductionTimeRegistration]    Script Date: 03/03/2026 16:26:08 ******/













CREATE OR REPLACE VIEW `DWH`.`V_FactProductionTimeRegistration` AS


SELECT    UPPER(ProductionOrderCode) AS ProductionOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(ProductConfigurationCode) AS ProductConfigurationCode
		, UPPER(ResourceCode) AS ResourceCode
		, UPPER(RouteCode) AS RouteCode
		, UPPER(OperationCode) AS OperationCode
		, OperationNumber
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, ETL.fn_DateKeyInt(PostedJournalDate) AS DimPostedJournalDateId
		, RoutingName
		, Shift
		, OperatorType
		, OperatorName
		, MachineTimeMinutes
		, MachineTimeHours
		, MachineTimeDays
		, MachineCostTC
		, MachineCostAC
		, MachineCostRC
		, MachineCostGC
		, MachineCostAC_Budget
		, MachineCostRC_Budget
		, MachineCostGC_Budget
		, OperatorTimeMinutes
		, OperatorTimeHours
		, OperatorTimeDays
		, LabourCostTC
		, LabourCostAC
		, LabourCostRC
		, LabourCostGC
		, LabourCostAC_Budget
		, LabourCostRC_Budget
		, LabourCostGC_Budget
		, AppliedExchangeRateTC
		, AppliedExchangeRateRC
		, AppliedExchangeRateAC
		, AppliedExchangeRateGC
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore2.ProductionTimeRegistration
;
