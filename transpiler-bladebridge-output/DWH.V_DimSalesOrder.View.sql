/****** Object:  View [DWH].[V_DimSalesOrder]    Script Date: 03/03/2026 16:26:08 ******/








CREATE OR REPLACE VIEW `DWH`.`V_DimSalesOrder` AS


SELECT	DISTINCT 
		  UPPER(SalesOrderCode) AS SalesOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, DeliveryAddress
		, RequestedShippingDate AS RequestedShippingDate
		, RequestedDeliveryDate AS RequestedDeliveryDate

FROM DataStore3.SalesOrder

/* Create unknown member */

UNION ALL

SELECT   '_N/A' AS SalesOrderCode
		, UPPER(CompanyCode) AS CompanyCode
		, '_N/A' AS DeliveryAddress
		, '1900-01-01' AS RequestedShippingDate
		, '1900-01-01' AS RequestedDeliveryDate

FROM DataStore.Company
;
