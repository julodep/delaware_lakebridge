/****** Object:  View [DataStore].[V_ShipmentInvoice]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_ShipmentInvoice` AS 


SELECT
	
	 COALESCE(NULLIF(SDS.BUSINESSOWNER , ''), '_N/A') AS BusinessOwnerCode
	, COALESCE(NULLIF(SDS.DATAAREAID, ''), '_N/A')  AS CompanyCode
	, COALESCE(NULLIF(SDS.CUSTACCOUNT, ''), '_N/A')  AS CustomerCode
	, COALESCE(NULLIF(SDS.DEPARTMENT, ''), '_N/A')  AS DepartmentCode
	, COALESCE(NULLIF(SDS.DESTINATIONAGENT, ''), '_N/A')  AS DestinationAgentCode
	, COALESCE(NULLIF(SDS.JOBOWNER, ''), '_N/A')  AS JobOwnerCode
	, COALESCE(NULLIF(SDS.LINEOFBUSINESSID, ''), '_N/A')  AS LineOfBusinessCode
	, COALESCE(NULLIF(SDS.MODEOFTRANSPORTID, ''), '_N/A')  AS ModeOfTransportCode
	, COALESCE(NULLIF(SIDS.VENDINVOICEID, ''), '_N/A')  AS PurchaseInvoiceCode
	, COALESCE(NULLIF(SIDS.CUSTINVOICEID, ''), '_N/A')  AS SalesInvoiceCode
	, COALESCE(NULLIF(SDS.SHIPMENT, ''), '_N/A')  AS ShipmentContractCode
	
	, COALESCE(NULLIF(CAST(SIDS.CURRENCYCODE AS STRING), ''), '_N/A')  AS TransactionCurrencyCode
	--, ISNULL(NULLIF(SDS.CUSTNAME, '') , N'_N/A')  AS CustomerName
	, COALESCE(NULLIF(SDS.MASTERBILLOFLADING , ''), '_N/A') AS MasterBillOfLading
	, COALESCE(NULLIF(SDS.HOUSEBILLOFLADING , ''), '_N/A') AS HouseBillOfLading
	, COALESCE(NULLIF(SDS.PORTOFDESTINATION, ''), '_N/A')  AS PortOfDestination
	, COALESCE(NULLIF(SDS.PORTOFORIGIN, ''), '_N/A')  AS PortOfOrigin
	, CAST(COALESCE(NULLIF(SDS.ETD , ''), '1900-01-01') AS DATE)AS Etd
	, CAST(COALESCE(NULLIF(SDS.ETA ,''), '1900-01-01') AS DATE) AS Eta
	, COALESCE(NULLIF(SDS.DESCRIPTION , ''), '_N/A') AS Description
	, COALESCE(NULLIF(SDS.BRANCH, ''), '_N/A') AS Branch
	, COALESCE(NULLIF(SDS.REMARK , ''), '_N/A') AS Remark
	, COALESCE(SIDS.LINENUM, 0) AS ShipmentInvoiceLineNumber
	, CAST(COALESCE(NULLIF(SIDS.INVOICEDATE , ''), '1900-01-01') AS DATE) AS ShipmentInvoiceDate
	, COALESCE(SIDS.AMOUNT, '0') AS Amount
	, COALESCE(NULLIF(SIDS.VOUCHER , ''), '_N/A') AS Voucher
FROM dbo.YSLEShipmentDetailsStaging SDS
LEFT JOIN dbo.YSLEShipmentInvoiceDetailsStaging SIDS
	ON SDS.Shipment = SIDS.ShipmentId 
		AND SDS.DataAreaId = SIDS.DATAAREAID
;
