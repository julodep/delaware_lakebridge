/****** Object:  View [DWH].[V_FactInventoryMovements]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DWH`.`V_FactInventoryMovements` AS


SELECT  UPPER(CompanyCode) AS CompanyCode
      , UPPER(ProductCode) AS ProductCode
      , UPPER(WarehouseLocationCode) AS WarehouseLocationCode
      , UPPER(InventLocationCode) AS InventLocationCode
      , UPPER(InventBatchCode) AS InventBatchCode 
	  , UPPER(InventDimCode) AS InventDimCode
	  , UPPER(SalesInvoiceCode) AS SalesInvoiceCode
      , ETL.fn_DateKeyInt(DatePhysical) AS DimDatePhysicalId
      , ETL.fn_DateKeyInt(DateFinancial) AS DimDateFinancialId
      , ETL.fn_DateKeyInt(DateClosed) AS DimDateClosedId
      , InventoryUnit
      , Quantity_InventoryUnit
      , Quantity_SalesUnit
	  , Quantity_PurchaseUnit
      , Currency AS CurrencyCode
      , CostPhysicalTC
      , CostPhysicalAC
      , CostPhysicalRC
      , CostPhysicalGC
      , CostFinancialTC
      , CostFinancialAC
      , CostFinancialRC
      , CostFinancialGC
      , PriceMatch
      , AppliedExchangeRateTC
      , AppliedExchangeRateRC
      , AppliedExchangeRateAC
      , AppliedExchangeRateGC

FROM DataStore3.InventoryMovements
;
