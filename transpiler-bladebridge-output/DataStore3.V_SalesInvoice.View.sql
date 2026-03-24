/****** Object:  View [DataStore3].[V_SalesInvoice]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DataStore3`.`V_SalesInvoice` AS 


SELECT    CONCAT(SI.SalesInvoiceCode, SI.SalesInvoiceLineNumber, SI.CompanyCode) AS SalesInvoiceIdScreening
		, SI.SalesInvoiceCode
		, SI.SalesInvoiceLineNumber
		, SI.SalesInvoiceLineNumberCombination
		, SI.CompanyCode
		, SI.SalesOrderCode
		, SI.SalesOrderStatus
		, SI.InventTransCode
		, SI.InventDimCode
		, SI.TaxWriteCode
		, SI.CustomerCode
		, SI.ProductCode
		, SI.PaymentTermsCode
		, SI.DeliveryModeCode
		, SI.DeliveryTermsCode
		, SI.OrderCustomerCode
		, SI.DefaultExchangeRateTypeCode
		, SI.BudgetExchangeRateTypeCode
		, SI.TransactionType
		, SI.TransactionCurrencyCode
		, SI.AccountingCurrencyCode
		, SI.ReportingCurrencyCode
		, SI.GroupCurrencyCode
		, SI.GLAccountCode
		, SI.IntercompanyCode
		, SI.BusinessSegmentCode
		, SI.DeparmentCode
		, SI.EndCustomerCode
		, SI.LocationCode
		, SI.ShipmentContractCode
		, SI.LocalAccountCode
		, SI.ProductFDCode
		--, SI.CostCenterCode
		, SI.InvoiceDate
		, SI.RequestedDeliveryDate
		, SI.ConfirmedDeliveryDate

		, SI.SalesUnit

		, SI.InvoicedQuantity_InventoryUnit
		, SI.InvoicedQuantity_PurchaseUnit
		, SI.InvoicedQuantity_SalesUnit

		, SI.SalesPricePerUnitTC
		, SI.SalesPricePerUnitAC
		, SI.SalesPricePerUnitRC
		, SI.SalesPricePerUnitGC
		, SI.SalesPricePerUnitAC_Budget
		, SI.SalesPricePerUnitRC_Budget
		, SI.SalesPricePerUnitGC_Budget
		, SI.GrossSalesTC
		, SI.GrossSalesAC
		, SI.GrossSalesRC
		, SI.GrossSalesGC
		, SI.GrossSalesAC_Budget
		, SI.GrossSalesRC_Budget
		, SI.GrossSalesGC_Budget
		, SI.DiscountAmountTC
		, SI.DiscountAmountAC
		, SI.DiscountAmountRC
		, SI.DiscountAmountGC
		, SI.DiscountAmountAC_Budget
		, SI.DiscountAmountRC_Budget
		, SI.DiscountAmountGC_Budget
		, SI.InvoicedSalesAmountTC
		, SI.InvoicedSalesAmountAC
		, SI.InvoicedSalesAmountRC
		, SI.InvoicedSalesAmountGC
		, InvoicedSalesAmountInclTaxTC = CAST(InvoicedSalesAmountTC * (1 + CAST(TaxWriteCode AS decimal(32,6))/100) AS DECIMAL(38,17))
		, InvoicedSalesAmountInclTaxAC = CAST(InvoicedSalesAmountAC * (1 + CAST(TaxWriteCode AS decimal(32,6))/100) AS DECIMAL(38,17)) 
		, InvoicedSalesAmountInclTaxRC = CAST(InvoicedSalesAmountRC * (1 + CAST(TaxWriteCode AS decimal(32,6))/100) AS DECIMAL(38,17)) 
		, InvoicedSalesAmountInclTaxGC = CAST(InvoicedSalesAmountGC * (1 + CAST(TaxWriteCode AS decimal(32,6))/100) AS DECIMAL(38,17)) 
		, SI.InvoicedSalesAmountAC_Budget
		, SI.InvoicedSalesAmountRC_Budget
		, SI.InvoicedSalesAmountGC_Budget
		, NetSalesAmountTC = NetSalesAmountTC
								- RebateAmountCompletedTC
								- RebateAmountMarkedTC
								- RebateAmountVarianceTC
		, NetSalesAmountAC = NetSalesAmountAC
								- RebateAmountCompletedAC
								- RebateAmountMarkedAC
								- RebateAmountVarianceAC
		, NetSalesAmountRC = NetSalesAmountRC
								- RebateAmountCompletedRC
								- RebateAmountMarkedRC
								- RebateAmountVarianceRC
		, NetSalesAmountGC = NetSalesAmountGC
								- RebateAmountCompletedGC
								- RebateAmountMarkedGC
								- RebateAmountVarianceGC
		, NetSalesAmountAC_Budget = NetSalesAmountAC_Budget
									- RebateAmountCompletedAC_Budget
									- RebateAmountMarkedAC_Budget
									- RebateAmountVarianceAC_Budget
		, NetSalesAmountRC_Budget = NetSalesAmountRC_Budget
									- RebateAmountCompletedRC_Budget
									- RebateAmountMarkedRC_Budget
									- RebateAmountVarianceRC_Budget
		, NetSalesAmountGC_Budget = NetSalesAmountGC_Budget
									- RebateAmountCompletedGC_Budget
									- RebateAmountMarkedGC_Budget
									- RebateAmountVarianceGC_Budget
		, SI. RebateAmountOriginalTC
		, SI. RebateAmountOriginalAC
		, SI. RebateAmountOriginalRC
		, SI. RebateAmountOriginalGC
		, SI. RebateAmountOriginalAC_Budget
		, SI. RebateAmountOriginalRC_Budget
		, SI. RebateAmountOriginalGC_Budget
		, SI. RebateAmountCompletedTC
		, SI. RebateAmountCompletedAC
		, SI. RebateAmountCompletedRC
		, SI. RebateAmountCompletedGC
		, SI. RebateAmountCompletedAC_Budget
		, SI. RebateAmountCompletedRC_Budget
		, SI. RebateAmountCompletedGC_Budget
		, SI. RebateAmountMarkedTC
		, SI. RebateAmountMarkedAC
		, SI. RebateAmountMarkedRC
		, SI. RebateAmountMarkedGC
		, SI. RebateAmountMarkedAC_Budget
		, SI. RebateAmountMarkedRC_Budget
		, SI. RebateAmountMarkedGC_Budget
		, SI. RebateAmountCancelledTC
		, SI. RebateAmountCancelledAC
		, SI. RebateAmountCancelledRC
		, SI. RebateAmountCancelledGC
		, SI. RebateAmountCancelledAC_Budget
		, SI. RebateAmountCancelledRC_Budget
		, SI. RebateAmountCancelledGC_Budget
		, SI. RebateAmountVarianceTC
		, SI. RebateAmountVarianceAC
		, SI. RebateAmountVarianceRC
		, SI. RebateAmountVarianceGC
		, SI. RebateAmountVarianceAC_Budget
		, SI. RebateAmountVarianceRC_Budget
		, SI. RebateAmountVarianceGC_Budget
		, SI.CostOfGoodsSoldTC
		, SI.CostOfGoodsSoldAC
		, SI.CostOfGoodsSoldRC
		, SI.CostOfGoodsSoldGC
		, SI.CostOfGoodsSoldAC_Budget
		, SI.CostOfGoodsSoldRC_Budget
		, SI.CostOfGoodsSoldGC_Budget
		, GrossMarginTC = NetSalesAmountTC
								 - RebateAmountCompletedTC
								 - RebateAmountMarkedTC
								 - RebateAmountVarianceTC
								 - CostOfGoodsSoldTC
		, GrossMarginAC = NetSalesAmountAC
								 - RebateAmountCompletedAC
								 - RebateAmountMarkedAC
								 - RebateAmountVarianceAC
								 - CostOfGoodsSoldAC
		, GrossMarginRC = NetSalesAmountRC
								 - RebateAmountCompletedRC
								 - RebateAmountMarkedRC
								 - RebateAmountVarianceRC
								 - CostOfGoodsSoldRC
		, GrossMarginGC = NetSalesAmountGC
								 - RebateAmountCompletedGC
								 - RebateAmountMarkedGC
								 - RebateAmountVarianceGC
								 - CostOfGoodsSoldGC
		, GrossMarginAC_Budget = NetSalesAmountAC_Budget
										- RebateAmountCompletedAC_Budget
										- RebateAmountMarkedAC_Budget
										- RebateAmountVarianceAC_Budget
										- CostOfGoodsSoldAC_Budget
		, GrossMarginRC_Budget = NetSalesAmountRC_Budget
										- RebateAmountCompletedRC_Budget
										- RebateAmountMarkedRC_Budget
										- RebateAmountVarianceRC_Budget
										- CostOfGoodsSoldRC_Budget
		, GrossMarginGC_Budget = NetSalesAmountGC_Budget
										- RebateAmountCompletedGC_Budget
										- RebateAmountMarkedGC_Budget
										- RebateAmountVarianceGC_Budget
										- CostOfGoodsSoldGC_Budget
		, SurchargeTotalTC
		, SurchargeTotalAC
		, SurchargeTotalRC
		, SurchargeTotalGC
		, SurchargeTotalAC_Budget
		, SurchargeTotalRC_Budget
		, SurchargeTotalGC_Budget
		, SI.AppliedExchangeRateTC
		, SI.AppliedExchangeRateAC
		, SI.AppliedExchangeRateRC
		, SI.AppliedExchangeRateGC
		, SI.AppliedExchangeRateAC_Budget
		, SI.AppliedExchangeRateRC_Budget
		, SI.AppliedExchangeRateGC_Budget

FROM DataStore2.SalesInvoice SI
;
