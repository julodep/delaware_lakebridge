/****** Object:  View [DataStore2].[V_ShipmentInvoice]    Script Date: 03/03/2026 16:26:09 ******/





/****** Script for SelectTopNRows command from SSMS  ******/

CREATE OR REPLACE VIEW `DataStore2`.`V_ShipmentInvoice` AS 

SELECT  
      BusinessOwnerCode
      ,CompanyCode
      ,DepartmentCode
      ,DestinationAgentCode
      ,JobOwnerCode
      ,LineOfBusinessCode
      ,ModeOfTransportCode
     
      ,PurchaseInvoiceCode
      ,SalesInvoiceCode
      ,ShipmentContractCode
      ,TransactionCurrencyCode
	  , L.ACCOUNTINGCURRENCY AS AccountingCurrencyCode
	  , L.REPORTINGCURRENCY AS ReportingCurrencyCode
	  , L.GroupCurrency AS GroupCurrencyCode
      ,CustomerCode
      --,CustomerName
      ,MasterBillOfLading
      ,HouseBillOfLading
      ,Etd
      ,Eta
      ,Description
	  ,Branch
      ,Remark
	  ,PortOfDestination
      ,PortOfOrigin
      ,ShipmentInvoiceLineNumber
      ,ShipmentInvoiceDate
      ,Amount
      ,Voucher
	  , ShipInv.Amount AS ShipmentInvoiceAmountTC
	  ,COALESCE(CASE WHEN ShipInv.TransactionCurrencyCode = L.AccountingCurrency THEN ShipInv.Amount 
ELSE ShipInv.Amount * AC.ExchangeRate END, 0) AS ShipmentInvoiceAmountAC
	  ,COALESCE(CASE WHEN ShipInv.TransactionCurrencyCode = L.ReportingCurrency THEN ShipInv.Amount 
ELSE ShipInv.Amount * RC.ExchangeRate END, 0) AS ShipmentInvoiceAmountRC
	 ,COALESCE(CASE WHEN ShipInv.TransactionCurrencyCode = L.GroupCurrency THEN ShipInv.Amount 
ELSE ShipInv.Amount * GC.ExchangeRate END, 0) AS ShipmentInvoiceAmountGC

  FROM DataStore.ShipmentInvoice ShipInv


  INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
				   , CASE WHEN LES.ReportingCurrency = ''
						  THEN G.GroupCurrencyCode
						  ELSE LES.REPORTINGCURRENCY
					 END AS ReportingCurrency
				   , LES.`Name`
				   , LES.ExchangeRateType
				   , LES.BudgetExchangeRateType
				   , G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON ShipInv.CompanyCode = L.`Name`

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = ShipInv.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND ShipInv.ShipmentInvoiceDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurreny
ON RC.FromCurrencyCode = ShipInv.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND ShipInv.ShipmentInvoiceDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = ShipInv.TransactionCurrencyCode 
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND ShipInv.ShipmentInvoiceDate BETWEEN GC.ValidFrom AND GC.ValidTo
;
