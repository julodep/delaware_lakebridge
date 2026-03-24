/****** Object:  View [DWH].[V_FactGeneralLedgerMerge]    Script Date: 03/03/2026 16:26:08 ******/








/****** Script for SelectTopNRows command from SSMS  ******/
CREATE OR REPLACE VIEW `DWH`.`V_FactGeneralLedgerMerge` AS 


SELECT FactGeneralLedgerId,
      DimCompanyId
      ,DimTransactionCurrencyId
      ,DimAccountingCurrencyId
      ,DimReportingCurrencyId
      ,DimGroupCurrencyId
      ,DimGLAccountId
      ,DimIntercompanyId
      ,DimPostingDateId
	  ,DimSupplierId
      ,DocumentDate
      ,RecId
      ,TransactionText
      ,TransactionCode
      ,Voucher
      ,AmountTC
      ,AmountAC
      ,AmountRC
      ,AmountGC
      ,AmountAC_Budget
      ,AmountRC_Budget
      ,AmountGC_Budget
      ,AppliedExchangeRateTC
      ,AppliedExchangeRateAC
      ,AppliedExchangeRateRC
      ,AppliedExchangeRateGC
      ,AppliedExchangeRateAC_Budget
      ,AppliedExchangeRateRC_Budget
      ,AppliedExchangeRateGC_Budget
      ,CreatedETLRunId
      ,ModifiedETLRunId
      ,DimBusinessSegmentId
      ,DimDepartmentId
      ,DimEndCustomerId
      ,DimLocationId
      ,DimShipmentContractId
      ,DimLocalAccountId
      ,DimProductFDId
	  ,DimVoucherId

  FROM DWH.FactGeneralLedger

  UNION ALL

  SELECT --FactHistoricGeneralLedgerId
  -1,
      FACT.DimCompanyId
      ,FACT.DimTransactionCurrencyId
      ,FACT.DimAccountingCurrencyId
      ,FACT.DimReportingCurrencyId
      ,FACT.DimGroupCurrencyId
      ,FACT.DimGLAccountId
      ,FACT.DimIntercompanyId
      ,FACT.DimPostingDateId
	  ,FACT.DimSupplierId
      ,FACT.DocumentDate
      ,FACT.RecId
      ,FACT.TransactionText
      ,FACT.TransactionCode
      ,FACT.Voucher
      ,FACT.AmountTC
      ,FACT.AmountAC
      ,FACT.AmountRC
      ,FACT.AmountGC
      ,FACT.AmountAC_Budget
      ,FACT.AmountRC_Budget
      ,FACT.AmountGC_Budget
      ,FACT.AppliedExchangeRateTC
      ,FACT.AppliedExchangeRateAC
      ,FACT.AppliedExchangeRateRC
      ,FACT.AppliedExchangeRateGC
      ,FACT.AppliedExchangeRateAC_Budget
      ,FACT.AppliedExchangeRateRC_Budget
      ,FACT.AppliedExchangeRateGC_Budget
      ,FACT.CreatedETLRunId
      ,FACT.ModifiedETLRunId
      ,FACT.DimBusinessSegmentId
      ,FACT.DimDepartmentId
      ,FACT.DimEndCustomerId
      ,FACT.DimLocationId
      ,FACT.DimShipmentContractId
      ,FACT.DimLocalAccountId
      ,FACT.DimProductFDId
	  ,VOU.DimVoucherId

	   FROM DWH.FactHistoricGeneralLedger FACT
	   LEFT JOIN DWH.DimVoucher VOU
ON VOU.Voucher= '_N/A'
AND VOU.CompanyCode = 'BE20'

;
