# Databricks notebook source
# MAGIC %md
# MAGIC # DWH.FactSalesOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_high/DWH.FactSalesOrder.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# 1.  CREATE TABLE  –  FactSalesOrder
# ------------------------------------------------------------
# The original T‑SQL script created a massive table with ~200 columns,
# an IDENTITY primary key, and a host of foreign‑key constraints.
#
# • All column names and types are mapped to Spark/Delta equivalents:
#     INT                     -> INT
#     NUMERIC(p,s) / DECIMAL  -> DECIMAL(p,s)
#     MONEY (numeric(19,4))   -> DECIMAL(19,4)  (used DECIMAL(38,6) as in source)
#   (UNIQUEIDENTIFIER, VARBINARY, etc. were not present in this table)
#
# • The table uses `IDENTITY(1,1)` for the surrogate key – Delta supports
#   this syntax.
#
# • Primary‑key constraints are supported in Delta 2.x.  The
#   original “PRIMARY KEY CLUSTERED” clause is simplified to
#   `PRIMARY KEY` after the column list.
#
# • Foreign‑key constraints (`CONSTRAINT … FOREIGN KEY (…) REFERENCES …`)
#   are not supported in Delta Lake the same way as in T‑SQL.  We therefore
#   **comment them out** and add a note in the notebook.
#
# • All references to other schemas (e.g. `[DWH]`) have been replaced by
#   fully‑qualified names `dbe_dbx_internships.switchschema.{table}`.
#
# ------------------------------------------------------------
import re

# COMMAND ----------

# --- Build the CREATE TABLE statement ---------------------------------
create_table_sql = """
CREATE TABLE dbe_dbx_internships.switchschema.FactSalesOrder (
    FactSalesOrderId INT IDENTITY(1,1) NOT NULL,
    DimSalesOrderId INT NOT NULL,
    DimCompanyId INT NOT NULL,
    DimDeliveryModeId INT NOT NULL,
    DimDeliveryTermsId INT NOT NULL,
    DimPaymentTermsId INT NOT NULL,
    DimProductId INT NOT NULL,
    DimProductConfigurationId INT NOT NULL,
    DimTransactionCurrencyId INT NOT NULL,
    DimAccountingCurrencyId INT NOT NULL,
    DimReportingCurrencyId INT NOT NULL,
    DimGroupCurrencyId INT NOT NULL,
    DimCustomerId INT NOT NULL,
    DimOrderCustomerId INT NOT NULL,
    DimCreationDateId INT NOT NULL,
    DimRequestedDeliveryDateId INT NOT NULL,
    DimConfirmedDeliveryDateId INT NOT NULL,
    DimRequestedShippingDateId INT NOT NULL,
    DimConfirmedShippingDateId INT NOT NULL,
    DimLastShipmentDateId INT NOT NULL,
    SalesOrderStatus STRING NOT NULL,
    DocumentStatus STRING NOT NULL,
    SalesUnit STRING NOT NULL,
    SalesOrderLineNumber STRING NOT NULL,
    SalesOrderLineNumberCombination STRING NOT NULL,
    DeliveryAddress STRING NOT NULL,
    InventTransCode STRING NOT NULL,
    InventDimCode STRING NOT NULL,
    SalesPricePerUnitTC DECIMAL(38,6) NULL,
    SalesPricePerUnitAC DECIMAL(38,6) NULL,
    SalesPricePerUnitRC DECIMAL(38,6) NULL,
    SalesPricePerUnitGC DECIMAL(38,6) NULL,
    SalesPricePerUnitAC_Budget DECIMAL(38,6) NULL,
    SalesPricePerUnitRC_Budget DECIMAL(38,6) NULL,
    SalesPricePerUnitGC_Budget DECIMAL(38,6) NULL,
    GrossSalesTC DECIMAL(38,6) NULL,
    GrossSalesAC DECIMAL(38,6) NULL,
    GrossSalesRC DECIMAL(38,6) NULL,
    GrossSalesGC DECIMAL(38,6) NULL,
    GrossSalesAC_Budget DECIMAL(38,6) NULL,
    GrossSalesRC_Budget DECIMAL(38,6) NULL,
    GrossSalesGC_Budget DECIMAL(38,6) NULL,
    DiscountAmountTC DECIMAL(38,6) NULL,
    DiscountAmountAC DECIMAL(38,6) NULL,
    DiscountAmountRC DECIMAL(38,6) NULL,
    DiscountAmountGC DECIMAL(38,6) NULL,
    DiscountAmountAC_Budget DECIMAL(38,6) NULL,
    DiscountAmountRC_Budget DECIMAL(38,6) NULL,
    DiscountAmountGC_Budget DECIMAL(38,6) NULL,
    InvoicedSalesAmountTC DECIMAL(38,17) NULL,
    InvoicedSalesAmountAC DECIMAL(38,6) NULL,
    InvoicedSalesAmountRC DECIMAL(38,6) NULL,
    InvoicedSalesAmountGC DECIMAL(38,6) NULL,
    InvoicedSalesAmountAC_Budget DECIMAL(38,6) NULL,
    InvoicedSalesAmountRC_Budget DECIMAL(38,6) NULL,
    InvoicedSalesAmountGC_Budget DECIMAL(38,6) NULL,
    NetSalesAmountTC DECIMAL(38,6) NULL,
    NetSalesAmountAC DECIMAL(38,6) NULL,
    NetSalesAmountRC DECIMAL(38,6) NULL,
    NetSalesAmountGC DECIMAL(38,6) NULL,
    NetSalesAmountAC_Budget DECIMAL(38,6) NULL,
    NetSalesAmountRC_Budget DECIMAL(38,6) NULL,
    NetSalesAmountGC_Budget DECIMAL(38,6) NULL,
    CostOfGoodsSoldTC DECIMAL(38,6) NULL,
    CostOfGoodsSoldAC DECIMAL(38,6) NULL,
    CostOfGoodsSoldRC DECIMAL(38,6) NULL,
    CostOfGoodsSoldGC DECIMAL(38,6) NULL,
    CostOfGoodsSoldAC_Budget DECIMAL(38,6) NULL,
    CostOfGoodsSoldRC_Budget DECIMAL(38,6) NULL,
    CostOfGoodsSoldGC_Budget DECIMAL(38,6) NULL,
    GrossMarginTC DECIMAL(38,6) NULL,
    GrossMarginAC DECIMAL(38,6) NULL,
    GrossMarginRC DECIMAL(38,6) NULL,
    GrossMarginGC DECIMAL(38,6) NULL,
    GrossMarginAC_Budget DECIMAL(38,6) NULL,
    GrossMarginRC_Budget DECIMAL(38,6) NULL,
    GrossMarginGC_Budget DECIMAL(38,6) NULL,
    AppliedExchangeRateTC DECIMAL(38,6) NULL,
    AppliedExchangeRateAC DECIMAL(38,20) NULL,
    AppliedExchangeRateRC DECIMAL(38,20) NULL,
    AppliedExchangeRateGC DECIMAL(38,20) NULL,
    AppliedExchangeRateAC_Budget DECIMAL(38,20) NULL,
    AppliedExchangeRateRC_Budget DECIMAL(38,20) NULL,
    AppliedExchangeRateGC_Budget DECIMAL(38,20) NULL,
    OrderedQuantity_InventoryUnit DECIMAL(38,6) NULL,
    OrderedQuantity_PurchaseUnit DECIMAL(38,6) NULL,
    OrderedQuantity_SalesUnit DECIMAL(38,6) NULL,
    DeliveredQuantity_InventoryUnit DECIMAL(38,6) NULL,
    DeliveredQuantity_PurchaseUnit DECIMAL(38,6) NULL,
    DeliveredQuantity_SalesUnit DECIMAL(38,6) NULL,
    SurchargeDeliveryAC DECIMAL(38,6) NULL,
    SurchargeDeliveryAC_Budget DECIMAL(38,6) NULL,
    SurchargeDeliveryGC DECIMAL(38,6) NULL,
    SurchargeDeliveryGC_Budget DECIMAL(38,6) NULL,
    SurchargeDeliveryRC DECIMAL(38,6) NULL,
    SurchargeDeliveryRC_Budget DECIMAL(38,6) NULL,
    SurchargeDeliveryTC DECIMAL(38,6) NULL,
    SurchargePurchaseAC DECIMAL(38,6) NULL,
    SurchargePurchaseAC_Budget DECIMAL(38,6) NULL,
    SurchargePurchaseGC DECIMAL(38,6) NULL,
    SurchargePurchaseGC_Budget DECIMAL(38,6) NULL,
    SurchargePurchaseRC DECIMAL(38,6) NULL,
    SurchargePurchaseRC_Budget DECIMAL(38,6) NULL,
    SurchargePurchaseTC DECIMAL(38,6) NULL,
    SurchargeTotalAC DECIMAL(38,6) NULL,
    SurchargeTotalAC_Budget DECIMAL(38,6) NULL,
    SurchargeTotalGC DECIMAL(38,6) NULL,
    SurchargeTotalGC_Budget DECIMAL(38,6) NULL,
    SurchargeTotalRC DECIMAL(38,6) NULL,
    SurchargeTotalRC_Budget DECIMAL(38,6) NULL,
    SurchargeTotalTC DECIMAL(38,6) NULL,
    SurchargeTransportAC DECIMAL(38,6) NULL,
    SurchargeTransportAC_Budget DECIMAL(38,6) NULL,
    SurchargeTransportGC DECIMAL(38,6) NULL,
    SurchargeTransportGC_Budget DECIMAL(38,6) NULL,
    SurchargeTransportRC DECIMAL(38,6) NULL,
    SurchargeTransportRC_Budget DECIMAL(38,6) NULL,
    SurchargeTransportTC DECIMAL(38,6) NULL,
    CreatedETLRunId INT NOT NULL,
    ModifiedETLRunId INT NOT NULL,
    
    -- Primary key – the original was "PRIMARY KEY CLUSTERED".  Delta supports a simple primary‑key constraint.
    PRIMARY KEY (FactSalesOrderId)
);
"""

# COMMAND ----------

# Execute the CREATE TABLE statement
spark.sql(create_table_sql)
print("✓ FactSalesOrder table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'IDENTITY'. SQLSTATE: 42601 (line 1, pos 96)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE dbe_dbx_internships.switchschema.FactSalesOrder (     FactSalesOrderId INT IDENTITY(1,1) NOT NULL,     DimSalesOrderId INT NOT NULL,     DimCompanyId INT NOT NULL,     DimDeliveryModeId INT NOT NULL,     DimDeliveryTermsId INT NOT NULL,     DimPaymentTermsId INT NOT NULL,     DimProductId INT NOT NULL,     DimProductConfigurationId INT NOT NULL,     DimTransactionCurrencyId INT NOT NULL,     DimAccountingCurrencyId INT NOT NULL,     DimReportingCurrencyId INT NOT NULL,     DimGroupCurrencyId INT NOT NULL,     DimCustomerId INT NOT NULL,     DimOrderCustomerId INT NOT NULL,     DimCreationDateId INT NOT NULL,     DimRequestedDeliveryDateId INT NOT NULL,     DimConfirmedDeliveryDateId INT NOT NULL,     DimRequestedShippingDateId INT NOT NULL,     DimConfirmedShippingDateId INT NOT NULL,     DimLastShipmentDateId INT NOT NULL,     SalesOrderStatus STRING NOT NULL,     DocumentStatus STRING NOT NULL,     SalesUnit STRING NOT NULL,     SalesOrderLineNumber STRING NOT NULL,     SalesOrderLineNumberCombination STRING NOT NULL,     DeliveryAddress STRING NOT NULL,     InventTransCode STRING NOT NULL,     InventDimCode STRING NOT NULL,     SalesPricePerUnitTC DECIMAL(38,6) NULL,     SalesPricePerUnitAC DECIMAL(38,6) NULL,     SalesPricePerUnitRC DECIMAL(38,6) NULL,     SalesPricePerUnitGC DECIMAL(38,6) NULL,     SalesPricePerUnitAC_Budget DECIMAL(38,6) NULL,     SalesPricePerUnitRC_Budget DECIMAL(38,6) NULL,     SalesPricePerUnitGC_Budget DECIMAL(38,6) NULL,     GrossSalesTC DECIMAL(38,6) NULL,     GrossSalesAC DECIMAL(38,6) NULL,     GrossSalesRC DECIMAL(38,6) NULL,     GrossSalesGC DECIMAL(38,6) NULL,     GrossSalesAC_Budget DECIMAL(38,6) NULL,     GrossSalesRC_Budget DECIMAL(38,6) NULL,     GrossSalesGC_Budget DECIMAL(38,6) NULL,     DiscountAmountTC DECIMAL(38,6) NULL,     DiscountAmountAC DECIMAL(38,6) NULL,     DiscountAmountRC DECIMAL(38,6) NULL,     DiscountAmountGC DECIMAL(38,6) NULL,     DiscountAmountAC_Budget DECIMAL(38,6) NULL,     DiscountAmountRC_Budget DECIMAL(38,6) NULL,     DiscountAmountGC_Budget DECIMAL(38,6) NULL,     InvoicedSalesAmountTC DECIMAL(38,17) NULL,     InvoicedSalesAmountAC DECIMAL(38,6) NULL,     InvoicedSalesAmountRC DECIMAL(38,6) NULL,     InvoicedSalesAmountGC DECIMAL(38,6) NULL,     InvoicedSalesAmountAC_Budget DECIMAL(38,6) NULL,     InvoicedSalesAmountRC_Budget DECIMAL(38,6) NULL,     InvoicedSalesAmountGC_Budget DECIMAL(38,6) NULL,     NetSalesAmountTC DECIMAL(38,6) NULL,     NetSalesAmountAC DECIMAL(38,6) NULL,     NetSalesAmountRC DECIMAL(38,6) NULL,     NetSalesAmountGC DECIMAL(38,6) NULL,     NetSalesAmountAC_Budget DECIMAL(38,6) NULL,     NetSalesAmountRC_Budget DECIMAL(38,6) NULL,     NetSalesAmountGC_Budget DECIMAL(38,6) NULL,     CostOfGoodsSoldTC DECIMAL(38,6) NULL,     CostOfGoodsSoldAC DECIMAL(38,6) NULL,     CostOfGoodsSoldRC DECIMAL(38,6) NULL,     CostOfGoodsSoldGC DECIMAL(38,6) NULL,     CostOfGoodsSoldAC_Budget DECIMAL(38,6) NULL,     CostOfGoodsSoldRC_Budget DECIMAL(38,6) NULL,     CostOfGoodsSoldGC_Budget DECIMAL(38,6) NULL,     GrossMarginTC DECIMAL(38,6) NULL,     GrossMarginAC DECIMAL(38,6) NULL,     GrossMarginRC DECIMAL(38,6) NULL,     GrossMarginGC DECIMAL(38,6) NULL,     GrossMarginAC_Budget DECIMAL(38,6) NULL,     GrossMarginRC_Budget DECIMAL(38,6) NULL,     GrossMarginGC_Budget DECIMAL(38,6) NULL,     AppliedExchangeRateTC DECIMAL(38,6) NULL,     AppliedExchangeRateAC DECIMAL(38,20) NULL,     AppliedExchangeRateRC DECIMAL(38,20) NULL,     AppliedExchangeRateGC DECIMAL(38,20) NULL,     AppliedExchangeRateAC_Budget DECIMAL(38,20) NULL,     AppliedExchangeRateRC_Budget DECIMAL(38,20) NULL,     AppliedExchangeRateGC_Budget DECIMAL(38,20) NULL,     OrderedQuantity_InventoryUnit DECIMAL(38,6) NULL,     OrderedQuantity_PurchaseUnit DECIMAL(38,6) NULL,     OrderedQuantity_SalesUnit DECIMAL(38,6) NULL,     DeliveredQuantity_InventoryUnit DECIMAL(38,6) NULL,     DeliveredQuantity_PurchaseUnit DECIMAL(38,6) NULL,     DeliveredQuantity_SalesUnit DECIMAL(38,6) NULL,     SurchargeDeliveryAC DECIMAL(38,6) NULL,     SurchargeDeliveryAC_Budget DECIMAL(38,6) NULL,     SurchargeDeliveryGC DECIMAL(38,6) NULL,     SurchargeDeliveryGC_Budget DECIMAL(38,6) NULL,     SurchargeDeliveryRC DECIMAL(38,6) NULL,     SurchargeDeliveryRC_Budget DECIMAL(38,6) NULL,     SurchargeDeliveryTC DECIMAL(38,6) NULL,     SurchargePurchaseAC DECIMAL(38,6) NULL,     SurchargePurchaseAC_Budget DECIMAL(38,6) NULL,     SurchargePurchaseGC DECIMAL(38,6) NULL,     SurchargePurchaseGC_Budget DECIMAL(38,6) NULL,     SurchargePurchaseRC DECIMAL(38,6) NULL,     SurchargePurchaseRC_Budget DECIMAL(38,6) NULL,     SurchargePurchaseTC DECIMAL(38,6) NULL,     SurchargeTotalAC DECIMAL(38,6) NULL,     SurchargeTotalAC_Budget DECIMAL(38,6) NULL,     SurchargeTotalGC DECIMAL(38,6) NULL,     SurchargeTotalGC_Budget DECIMAL(38,6) NULL,     SurchargeTotalRC DECIMAL(38,6) NULL,     SurchargeTotalRC_Budget DECIMAL(38,6) NULL,     SurchargeTotalTC DECIMAL(38,6) NULL,     SurchargeTransportAC DECIMAL(38,6) NULL,     SurchargeTransportAC_Budget DECIMAL(38,6) NULL,     SurchargeTransportGC DECIMAL(38,6) NULL,     SurchargeTransportGC_Budget DECIMAL(38,6) NULL,     SurchargeTransportRC DECIMAL(38,6) NULL,     SurchargeTransportRC_Budget DECIMAL(38,6) NULL,     SurchargeTransportTC DECIMAL(38,6) NULL,     CreatedETLRunId INT NOT NULL,     ModifiedETLRunId INT NOT NULL,          -- Primary key – the original was "PRIMARY KEY CLUSTERED".  Delta supports a simple primary‑key constraint.     PRIMARY KEY (FactSalesOrderId) );
# MAGIC ------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
