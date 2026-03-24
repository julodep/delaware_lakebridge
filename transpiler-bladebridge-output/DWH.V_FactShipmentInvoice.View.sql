/****** Object:  View [DWH].[V_FactShipmentInvoice]    Script Date: 03/03/2026 16:26:08 ******/








/****** Script for SelectTopNRows command from SSMS  ******/
CREATE OR REPLACE VIEW `DWH`.`V_FactShipmentInvoice` AS 

SELECT UPPER(TRIM(BusinessOwnerCode)) AS BusinessOwnerCode
      ,UPPER(CompanyCode) AS CompanyCode
      ,UPPER(DepartmentCode) AS DepartmentCode
      ,UPPER(DestinationAgentCode) AS DestinationAgentCode
      ,UPPER(JobOwnerCode) AS JobOwnerCode
      ,UPPER(LineOfBusinessCode) AS BusinessSegmentCode
      ,UPPER(ModeOfTransportCode) AS ModeOfTransportCode
      ,UPPER(PurchaseInvoiceCode) AS PurchaseInvoiceCode
      ,UPPER(SalesInvoiceCode) AS SalesInvoiceCode
      ,UPPER(ShipmentContractCode) AS ShipmentContractCode
      ,UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
      ,UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
      ,UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
      ,UPPER(GroupCurrencyCode) AS GroupCurrencyCode
      ,UPPER(CustomerCode) AS CustomerCode
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
      ,ETL.fn_DateKeyInt(ShipmentInvoiceDate)AS DimShipmentInvoiceDateId
      ,Voucher
      ,ShipmentInvoiceAmountTC
      ,ShipmentInvoiceAmountAC
      ,ShipmentInvoiceAmountRC
      ,ShipmentInvoiceAmountGC
  FROM DataStore2.ShipmentInvoice
;
