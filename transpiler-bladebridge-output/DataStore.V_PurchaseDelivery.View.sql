/****** Object:  View [DataStore].[V_PurchaseDelivery]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_PurchaseDelivery` AS



SELECT 
	  VPSJS.PackingSlipId AS PackingSlipCode	
/*Dimensions*/	
	, COALESCE(NULLIF(VPSJS.PurchId,''), '_N/A') AS PurchaseOrderCode
	, COALESCE(NULLIF(VPSJS.DataAreaId,''), '_N/A') AS CompanyCode
	, COALESCE(NULLIF(VPSTS.InventDimId,''), '_N/A') AS ProductConfigurationCode
	, COALESCE(NULLIF(VPSTS.ItemId,''), '_N/A') AS ProductCode
	, COALESCE(NULLIF(VPSJS.OrderAccount,''), '_N/A') AS OrderSupplierCode
	, COALESCE(NULLIF(VPSJS.InvoiceAccount,''), '_N/A') AS SupplierCode
	, COALESCE(NULLIF(VPSJS.DlvMode,''), '_N/A') AS DeliveryModeCode
	, COALESCE(NULLIF(VPSJS.DlvTerm,''), '_N/A') AS DeliveryTermsCode

/*Dates*/
	, CAST(COALESCE(VPSJS.DeliveryDate, '1900-01-01') AS DATE) AS ActualDeliveryDate
	, CAST(COALESCE(PO.RequestedDeliveryDate, '1900-01-01') AS DATE) AS RequestedDeliveryDate
	, CAST(COALESCE(PO.ConfirmedDeliveryDate, '1900-01-01') AS DATE) AS ConfirmedDeliveryDate

/*Delivery Details*/
	, CAST(COALESCE(NULLIF(StringMapPurchaseType.Name,''), '_N/A') AS STRING) AS PurchaseType
	, COALESCE(VPSTS.PurchaseLineLineNumber, -1) AS PurchaseOrderLineNumber	
	, COALESCE(NULLIF(VPSJS.DeliveryName,''), '_N/A') AS DeliveryName
	, COALESCE(VPSTS.LineNum, -1) AS DeliveryLineNumber
	, COALESCE(NULLIF(VPSTS.PurchUnit,''), '_N/A') AS PurchaseUnit
	, VPSTS.Ordered AS QuantityOrdered
	, VPSTS.Qty AS QuantityDelivered	

FROM dbo.SMRBIVendPackingSlipJourStaging AS VPSJS

LEFT JOIN dbo.SMRBIVendPackingSlipTransStaging AS VPSTS
ON VPSJS.VendPackingSlipJourRecId = VPSTS.VendPackingSlipJour
AND VPSJS.DataAreaId = VPSTS.DataAreaId

LEFT JOIN DataStore.PurchaseOrder AS PO
ON VPSJS.PurchId = PO.PurchaseOrderCode
AND VPSTS.PurchaseLineLineNumber = PO.PurchaseOrderLineNumber
AND VPSTS.DataAreaId = PO.CompanyCode

--/*To get enum labels*/
LEFT JOIN ETL.StringMap StringMapPurchaseType
ON StringMapPurchaseType.SourceTable = 'PurchaseType'
AND StringMapPurchaseType.Enum = Cast(VPSJS.PurchaseType as STRING)
;
