/****** Object:  View [DWH].[V_FactSalesInvoice]    Script Date: 03/03/2026 16:26:08 ******/














CREATE OR REPLACE VIEW `DWH`.`V_FactSalesInvoice` AS 


SELECT	  UPPER(SalesInvoiceCode) AS SalesInvoiceCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(SalesOrderCode) AS SalesOrderCode
		, UPPER(InventTransCode) AS InventTransCode
		, UPPER(InventDimCode) AS InventDimCode
		, UPPER(CustomerCode) AS CustomerCode
		, UPPER(OrderCustomerCode) AS OrderCustomerCode
		, UPPER(ProductCode) AS ProductCode
		, UPPER(PaymentTermsCode) AS PaymentTermsCode
		, UPPER(DeliveryModeCode) AS DeliveryModeCode
		, UPPER(DeliveryTermsCode) AS DeliveryTermsCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, UPPER(GLAccountCode) AS GLAccountCode
		, UPPER(IntercompanyCode) AS IntercompanyCode
		, UPPER(BusinessSegmentCode) AS BusinessSegmentCode
		, UPPER(DeparmentCode) AS DeparmentCode
		, UPPER(EndCustomerCode) AS EndCustomerCode
		, UPPER(LocationCode) AS LocationCode
		, UPPER(ShipmentContractCode) AS ShipmentContractCode
		, UPPER(LocalAccountCode) AS LocalAccountCode
		, UPPER(ProductFDCode) AS ProductFDCode
		--, UPPER(CostCenterCode) AS CostCenterCode

		, ETL.fn_DateKeyInt(InvoiceDate) AS DimInvoiceDateId
		, ETL.fn_DateKeyInt(RequestedDeliveryDate) AS DimRequestedDeliveryDateId
		, ETL.fn_DateKeyInt(ConfirmedDeliveryDate) AS DimConfirmedDeliveryDateId

		, ETL.fn_MonthKeyInt(InvoiceDate) AS InvoiceMonthId

		, SalesOrderStatus
		, TaxWriteCode 
		, SalesInvoiceLineNumber
		, SalesInvoiceLineNumberCombination
		, SalesUnit

		, InvoicedQuantity_InventoryUnit
		, InvoicedQuantity_PurchaseUnit
		, InvoicedQuantity_SalesUnit

		, SalesPricePerUnitTC
		, SalesPricePerUnitAC
		, SalesPricePerUnitRC
		, SalesPricePerUnitGC

		, SalesPricePerUnitAC_Budget
		, SalesPricePerUnitRC_Budget
		, SalesPricePerUnitGC_Budget

		, GrossSalesTC
		, GrossSalesAC
		, GrossSalesRC
		, GrossSalesGC

		, GrossSalesAC_Budget
		, GrossSalesRC_Budget
		, GrossSalesGC_Budget

		, DiscountAmountTC
		, DiscountAmountAC
		, DiscountAmountRC
		, DiscountAmountGC

		, DiscountAmountAC_Budget
		, DiscountAmountRC_Budget
		, DiscountAmountGC_Budget

		, InvoicedSalesAmountTC
		, InvoicedSalesAmountAC
		, InvoicedSalesAmountRC
		, InvoicedSalesAmountGC

		, InvoicedSalesAmountAC_Budget
		, InvoicedSalesAmountRC_Budget
		, InvoicedSalesAmountGC_Budget

		, InvoicedSalesAmountInclTaxAC
		, InvoicedSalesAmountInclTaxGC
		, InvoicedSalesAmountInclTaxRC
		, InvoicedSalesAmountInclTaxTC

		, RebateAmountCancelledAC
		, RebateAmountCancelledAC_Budget
		, RebateAmountCancelledGC
		, RebateAmountCancelledGC_Budget
		, RebateAmountCancelledRC
		, RebateAmountCancelledRC_Budget
		, RebateAmountCancelledTC

		, RebateAmountCompletedAC
		, RebateAmountCompletedAC_Budget
		, RebateAmountCompletedGC
		, RebateAmountCompletedGC_Budget
		, RebateAmountCompletedRC
		, RebateAmountCompletedRC_Budget
		, RebateAmountCompletedTC

		, RebateAmountMarkedAC
		, RebateAmountMarkedAC_Budget
		, RebateAmountMarkedGC
		, RebateAmountMarkedGC_Budget
		, RebateAmountMarkedRC
		, RebateAmountMarkedRC_Budget
		, RebateAmountMarkedTC

		, RebateAmountOriginalAC
		, RebateAmountOriginalAC_Budget
		, RebateAmountOriginalGC
		, RebateAmountOriginalGC_Budget
		, RebateAmountOriginalRC
		, RebateAmountOriginalRC_Budget
		, RebateAmountOriginalTC

		, RebateAmountVarianceAC
		, RebateAmountVarianceAC_Budget
		, RebateAmountVarianceGC
		, RebateAmountVarianceGC_Budget
		, RebateAmountVarianceRC
		, RebateAmountVarianceRC_Budget
		, RebateAmountVarianceTC

		, SurchargeTotalAC
		, SurchargeTotalAC_Budget
		, SurchargeTotalGC
		, SurchargeTotalGC_Budget
		, SurchargeTotalRC
		, SurchargeTotalRC_Budget
		, SurchargeTotalTC

		, CAST(NetSalesAmountTC AS DECIMAL(38,17)) AS NetSalesAmountTC
		, CAST(NetSalesAmountAC AS DECIMAL(38,17)) AS NetSalesAmountAC
		, CAST(NetSalesAmountRC AS DECIMAL(38,17)) AS NetSalesAmountRC
		, CAST(NetSalesAmountGC AS DECIMAL(38,17)) AS NetSalesAmountGC

		, CAST(NetSalesAmountAC_Budget AS DECIMAL(38,17)) AS NetSalesAmountAC_Budget
		, CAST(NetSalesAmountRC_Budget AS DECIMAL(38,17)) AS NetSalesAmountRC_Budget
		, CAST(NetSalesAmountGC_Budget AS DECIMAL(38,17)) AS NetSalesAmountGC_Budget

		, AppliedExchangeRateTC
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC

		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget

		, CostOfGoodsSoldTC
		, CostOfGoodsSoldAC
		, CostOfGoodsSoldRC
		, CostOfGoodsSoldGC

		, CostOfGoodsSoldAC_Budget
		, CostOfGoodsSoldRC_Budget
		, CostOfGoodsSoldGC_Budget

		, GrossMarginTC
		, GrossMarginAC
		, GrossMarginRC
		, GrossMarginGC

		, GrossMarginAC_Budget
		, GrossMarginRC_Budget
		, GrossMarginGC_Budget


FROM DataStore3.SalesInvoice
;
