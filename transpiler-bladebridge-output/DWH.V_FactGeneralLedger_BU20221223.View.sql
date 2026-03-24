/****** Object:  View [DWH].[V_FactGeneralLedger_BU20221223]    Script Date: 03/03/2026 16:26:08 ******/
















CREATE OR REPLACE VIEW `DWH`.`V_FactGeneralLedger_BU20221223` AS 

/*Adapt the financial dimensions!!*/
;
SELECT	  RecId
		, TransactionText
		, UPPER(TransactionCode) AS TransactionCode
		, UPPER(GL.CompanyCode) AS CompanyCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, GLAccountId
		, IntercompanyId
		--, CostCenterId
		 ,BusinessSegmentId
		 ,DepartmentId 
		, EndCustomerID
		, LocationId
		, ShipmentContractId
		, LocalAccountId
		, ProductFDId
		, VendorId
		, SUP.SupplierId
		, SUP.DimSupplierId
		, DocumentDate
		, ETL.fn_DateKeyInt(PostingDate) AS DimPostingDateId
		, IsDebitCredit
		, Voucher
		, AmountTC
		, AmountAC
		, AmountRC
		, AmountGC
		, AmountAC_Budget
		, AmountRC_Budget
		, AmountGC_Budget
		, AppliedExchangeRateTC
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore2.GeneralLedger GL

LEFT JOIN (SELECT DimSupplierId, CAST(SupplierID AS STRING) AS SUPPLIERID FROM DWH.DimSupplier SUP WHERE SupplierId <> -1 UNION SELECT DimSupplierId, CONCAT(CompanyCode, '|', SupplierId) AS SUPPLIERID FROM DWH.DimSupplier WHERE SupplierId = -1) SUP
ON SUP.SupplierId = CASE WHEN GL.VendorId = -1 THEN CONCAT(CompanyCode, '|', VendorId) ELSE CAST(VendorId AS STRING) END



;
