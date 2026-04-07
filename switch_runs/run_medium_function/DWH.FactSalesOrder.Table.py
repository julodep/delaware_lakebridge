# Databricks notebook source
# MAGIC %md
# MAGIC # DWH.FactSalesOrder.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_high/DWH.FactSalesOrder.Table.sql`

# COMMAND ----------

# ------------------------------------
#   Databricks Notebook – DDL Script
# ------------------------------------
# This notebook creates the Delta table `FactSalesOrder` in the catalog
# `dbe_dbx_internships.switchschema` that mirrors the T‑SQL definition you posted.
#
# IMPORTANT:
# • All identifiers are fully qualified:  `dbe_dbx_internships.switchschema.<object_name>`
# • The original T‑SQL used many foreign‑key constraints and an identity column.
#   Delta Lake does not support implicit identity values or referential‑integrity
#   constraints – they are therefore omitted.  In a production environment you
#   would enforce uniqueness / FK logic in your ETL pipeline or via downstream
#   validation (e.g. Delta Lake’s "MERGE" logic or "ACID" guarantees).
# • The column data‑types are mapped to their Spark/Delta equivalents:
#   - MS‑SQL BIGINT  -> BIGINT
#   - INT           -> INT
#   - NUMERIC(p,s)  -> DECIMAL(p,s)
#   - NVARCHAR(...) -> STRING
#   - NUMERIC(38,17) (high‑precision) is kept as DECIMAL(38,17)
#   - The primary‑key definition is commented out because Delta cannot enforce
#     it directly.
#
# Run the notebook cells to create the table.  If a previous version of the
# table exists you can replace it with `CREATE OR REPLACE` below.

# Drop the table if it already exists (optional – keeps the notebook idempotent)
spark.sql("""
DROP TABLE IF EXISTS dbe_dbx_internships.switchschema.FactSalesOrder
""")

# COMMAND ----------

# Create the table with the exact column list and data types
# -----------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.FactSalesOrder (
    -- Identity column in SQL SERVER: represented here as a regular BIGINT column.
    -- If you need an auto‑incrementing surrogate key, consider generating it
    -- in your ETL pipeline or using DDL‑PROGRESS's `GENERATED ALWAYS AS IDENTITY`.
    FactSalesOrderId BIGINT,

    DimSalesOrderId INT,
    DimCompanyId INT,
    DimDeliveryModeId INT,
    DimDeliveryTermsId INT,
    DimPaymentTermsId INT,
    DimProductId INT,
    DimProductConfigurationId INT,
    DimTransactionCurrencyId INT,
    DimAccountingCurrencyId INT,
    DimReportingCurrencyId INT,
    DimGroupCurrencyId INT,
    DimCustomerId INT,
    DimOrderCustomerId INT,
    DimCreationDateId INT,
    DimRequestedDeliveryDateId INT,
    DimConfirmedDeliveryDateId INT,
    DimRequestedShippingDateId INT,
    DimConfirmedShippingDateId INT,
    DimLastShipmentDateId INT,

    SalesOrderStatus STRING,
    DocumentStatus STRING,
    SalesUnit STRING,
    SalesOrderLineNumber STRING,
    SalesOrderLineNumberCombination STRING,
    DeliveryAddress STRING,
    InventTransCode STRING,
    InventDimCode STRING,

    SalesPricePerUnitTC DECIMAL(38,6),
    SalesPricePerUnitAC DECIMAL(38,6),
    SalesPricePerUnitRC DECIMAL(38,6),
    SalesPricePerUnitGC DECIMAL(38,6),
    SalesPricePerUnitAC_Budget DECIMAL(38,6),
    SalesPricePerUnitRC_Budget DECIMAL(38,6),
    SalesPricePerUnitGC_Budget DECIMAL(38,6),

    GrossSalesTC DECIMAL(38,6),
    GrossSalesAC DECIMAL(38,6),
    GrossSalesRC DECIMAL(38,6),
    GrossSalesGC DECIMAL(38,6),
    GrossSalesAC_Budget DECIMAL(38,6),
    GrossSalesRC_Budget DECIMAL(38,6),
    GrossSalesGC_Budget DECIMAL(38,6),

    DiscountAmountTC DECIMAL(38,6),
    DiscountAmountAC DECIMAL(38,6),
    DiscountAmountRC DECIMAL(38,6),
    DiscountAmountGC DECIMAL(38,6),
    DiscountAmountAC_Budget DECIMAL(38,6),
    DiscountAmountRC_Budget DECIMAL(38,6),
    DiscountAmountGC_Budget DECIMAL(38,6),

    InvoicedSalesAmountTC DECIMAL(38,17),
    InvoicedSalesAmountAC DECIMAL(38,6),
    InvoicedSalesAmountRC DECIMAL(38,6),
    InvoicedSalesAmountGC DECIMAL(38,6),
    InvoicedSalesAmountAC_Budget DECIMAL(38,6),
    InvoicedSalesAmountRC_Budget DECIMAL(38,6),
    InvoicedSalesAmountGC_Budget DECIMAL(38,6),

    NetSalesAmountTC DECIMAL(38,6),
    NetSalesAmountAC DECIMAL(38,6),
    NetSalesAmountRC DECIMAL(38,6),
    NetSalesAmountGC DECIMAL(38,6),
    NetSalesAmountAC_Budget DECIMAL(38,6),
    NetSalesAmountRC_Budget DECIMAL(38,6),
    NetSalesAmountGC_Budget DECIMAL(38,6),

    CostOfGoodsSoldTC DECIMAL(38,6),
    CostOfGoodsSoldAC DECIMAL(38,6),
    CostOfGoodsSoldRC DECIMAL(38,6),
    CostOfGoodsSoldGC DECIMAL(38,6),
    CostOfGoodsSoldAC_Budget DECIMAL(38,6),
    CostOfGoodsSoldRC_Budget DECIMAL(38,6),
    CostOfGoodsSoldGC_Budget DECIMAL(38,6),

    GrossMarginTC DECIMAL(38,6),
    GrossMarginAC DECIMAL(38,6),
    GrossMarginRC DECIMAL(38,6),
    GrossMarginGC DECIMAL(38,6),
    GrossMarginAC_Budget DECIMAL(38,6),
    GrossMarginRC_Budget DECIMAL(38,6),
    GrossMarginGC_Budget DECIMAL(38,6),

    AppliedExchangeRateTC DECIMAL(38,6),
    AppliedExchangeRateAC DECIMAL(38,20),
    AppliedExchangeRateRC DECIMAL(38,20),
    AppliedExchangeRateGC DECIMAL(38,20),
    AppliedExchangeRateAC_Budget DECIMAL(38,20),
    AppliedExchangeRateRC_Budget DECIMAL(38,20),
    AppliedExchangeRateGC_Budget DECIMAL(38,20),

    OrderedQuantity_InventoryUnit DECIMAL(38,6),
    OrderedQuantity_PurchaseUnit    DECIMAL(38,6),
    OrderedQuantity_SalesUnit       DECIMAL(38,6),

    DeliveredQuantity_InventoryUnit DECIMAL(38,6),
    DeliveredQuantity_PurchaseUnit   DECIMAL(38,6),
    DeliveredQuantity_SalesUnit      DECIMAL(38,6),

    SurchargeDeliveryAC          DECIMAL(38,6),
    SurchargeDeliveryAC_Budget   DECIMAL(38,6),
    SurchargeDeliveryGC          DECIMAL(38,6),
    SurchargeDeliveryGC_Budget   DECIMAL(38,6),
    SurchargeDeliveryRC          DECIMAL(38,6),
    SurchargeDeliveryRC_Budget   DECIMAL(38,6),
    SurchargeDeliveryTC          DECIMAL(38,6),

    SurchargePurchaseAC          DECIMAL(38,6),
    SurchargePurchaseAC_Budget   DECIMAL(38,6),
    SurchargePurchaseGC          DECIMAL(38,6),
    SurchargePurchaseGC_Budget   DECIMAL(38,6),
    SurchargePurchaseRC          DECIMAL(38,6),
    SurchargePurchaseRC_Budget   DECIMAL(38,6),
    SurchargePurchaseTC          DECIMAL(38,6),

    SurchargeTotalAC          DECIMAL(38,6),
    SurchargeTotalAC_Budget   DECIMAL(38,6),
    SurchargeTotalGC          DECIMAL(38,6),
    SurchargeTotalGC_Budget   DECIMAL(38,6),
    SurchargeTotalRC          DECIMAL(38,6),
    SurchargeTotalRC_Budget   DECIMAL(38,6),
    SurchargeTotalTC          DECIMAL(38,6),

    SurchargeTransportAC          DECIMAL(38,6),
    SurchargeTransportAC_Budget   DECIMAL(38,6),
    SurchargeTransportGC          DECIMAL(38,6),
    SurchargeTransportGC_Budget   DECIMAL(38,6),
    SurchargeTransportRC          DECIMAL(38,6),
    SurchargeTransportRC_Budget   DECIMAL(38,6),
    SurchargeTransportTC          DECIMAL(38,6),

    CreatedETLRunId INT NOT NULL,
    ModifiedETLRunId INT NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------
#   Verify – show the first rows
# ------------------------------------
display(spark.sql("SELECT * FROM dbe_dbx_internships.switchschema.FactSalesOrder LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 5133)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.FactSalesOrder (     -- Identity column in SQL SERVER: represented here as a regular BIGINT column.     -- If you need an auto‑incrementing surrogate key, consider generating it     -- in your ETL pipeline or using DDL‑PROGRESS's `GENERATED ALWAYS AS IDENTITY`.     FactSalesOrderId BIGINT,      DimSalesOrderId INT,     DimCompanyId INT,     DimDeliveryModeId INT,     DimDeliveryTermsId INT,     DimPaymentTermsId INT,     DimProductId INT,     DimProductConfigurationId INT,     DimTransactionCurrencyId INT,     DimAccountingCurrencyId INT,     DimReportingCurrencyId INT,     DimGroupCurrencyId INT,     DimCustomerId INT,     DimOrderCustomerId INT,     DimCreationDateId INT,     DimRequestedDeliveryDateId INT,     DimConfirmedDeliveryDateId INT,     DimRequestedShippingDateId INT,     DimConfirmedShippingDateId INT,     DimLastShipmentDateId INT,      SalesOrderStatus STRING,     DocumentStatus STRING,     SalesUnit STRING,     SalesOrderLineNumber STRING,     SalesOrderLineNumberCombination STRING,     DeliveryAddress STRING,     InventTransCode STRING,     InventDimCode STRING,      SalesPricePerUnitTC DECIMAL(38,6),     SalesPricePerUnitAC DECIMAL(38,6),     SalesPricePerUnitRC DECIMAL(38,6),     SalesPricePerUnitGC DECIMAL(38,6),     SalesPricePerUnitAC_Budget DECIMAL(38,6),     SalesPricePerUnitRC_Budget DECIMAL(38,6),     SalesPricePerUnitGC_Budget DECIMAL(38,6),      GrossSalesTC DECIMAL(38,6),     GrossSalesAC DECIMAL(38,6),     GrossSalesRC DECIMAL(38,6),     GrossSalesGC DECIMAL(38,6),     GrossSalesAC_Budget DECIMAL(38,6),     GrossSalesRC_Budget DECIMAL(38,6),     GrossSalesGC_Budget DECIMAL(38,6),      DiscountAmountTC DECIMAL(38,6),     DiscountAmountAC DECIMAL(38,6),     DiscountAmountRC DECIMAL(38,6),     DiscountAmountGC DECIMAL(38,6),     DiscountAmountAC_Budget DECIMAL(38,6),     DiscountAmountRC_Budget DECIMAL(38,6),     DiscountAmountGC_Budget DECIMAL(38,6),      InvoicedSalesAmountTC DECIMAL(38,17),     InvoicedSalesAmountAC DECIMAL(38,6),     InvoicedSalesAmountRC DECIMAL(38,6),     InvoicedSalesAmountGC DECIMAL(38,6),     InvoicedSalesAmountAC_Budget DECIMAL(38,6),     InvoicedSalesAmountRC_Budget DECIMAL(38,6),     InvoicedSalesAmountGC_Budget DECIMAL(38,6),      NetSalesAmountTC DECIMAL(38,6),     NetSalesAmountAC DECIMAL(38,6),     NetSalesAmountRC DECIMAL(38,6),     NetSalesAmountGC DECIMAL(38,6),     NetSalesAmountAC_Budget DECIMAL(38,6),     NetSalesAmountRC_Budget DECIMAL(38,6),     NetSalesAmountGC_Budget DECIMAL(38,6),      CostOfGoodsSoldTC DECIMAL(38,6),     CostOfGoodsSoldAC DECIMAL(38,6),     CostOfGoodsSoldRC DECIMAL(38,6),     CostOfGoodsSoldGC DECIMAL(38,6),     CostOfGoodsSoldAC_Budget DECIMAL(38,6),     CostOfGoodsSoldRC_Budget DECIMAL(38,6),     CostOfGoodsSoldGC_Budget DECIMAL(38,6),      GrossMarginTC DECIMAL(38,6),     GrossMarginAC DECIMAL(38,6),     GrossMarginRC DECIMAL(38,6),     GrossMarginGC DECIMAL(38,6),     GrossMarginAC_Budget DECIMAL(38,6),     GrossMarginRC_Budget DECIMAL(38,6),     GrossMarginGC_Budget DECIMAL(38,6),      AppliedExchangeRateTC DECIMAL(38,6),     AppliedExchangeRateAC DECIMAL(38,20),     AppliedExchangeRateRC DECIMAL(38,20),     AppliedExchangeRateGC DECIMAL(38,20),     AppliedExchangeRateAC_Budget DECIMAL(38,20),     AppliedExchangeRateRC_Budget DECIMAL(38,20),     AppliedExchangeRateGC_Budget DECIMAL(38,20),      OrderedQuantity_InventoryUnit DECIMAL(38,6),     OrderedQuantity_PurchaseUnit    DECIMAL(38,6),     OrderedQuantity_SalesUnit       DECIMAL(38,6),      DeliveredQuantity_InventoryUnit DECIMAL(38,6),     DeliveredQuantity_PurchaseUnit   DECIMAL(38,6),     DeliveredQuantity_SalesUnit      DECIMAL(38,6),      SurchargeDeliveryAC          DECIMAL(38,6),     SurchargeDeliveryAC_Budget   DECIMAL(38,6),     SurchargeDeliveryGC          DECIMAL(38,6),     SurchargeDeliveryGC_Budget   DECIMAL(38,6),     SurchargeDeliveryRC          DECIMAL(38,6),     SurchargeDeliveryRC_Budget   DECIMAL(38,6),     SurchargeDeliveryTC          DECIMAL(38,6),      SurchargePurchaseAC          DECIMAL(38,6),     SurchargePurchaseAC_Budget   DECIMAL(38,6),     SurchargePurchaseGC          DECIMAL(38,6),     SurchargePurchaseGC_Budget   DECIMAL(38,6),     SurchargePurchaseRC          DECIMAL(38,6),     SurchargePurchaseRC_Budget   DECIMAL(38,6),     SurchargePurchaseTC          DECIMAL(38,6),      SurchargeTotalAC          DECIMAL(38,6),     SurchargeTotalAC_Budget   DECIMAL(38,6),     SurchargeTotalGC          DECIMAL(38,6),     SurchargeTotalGC_Budget   DECIMAL(38,6),     SurchargeTotalRC          DECIMAL(38,6),     SurchargeTotalRC_Budget   DECIMAL(38,6),     SurchargeTotalTC          DECIMAL(38,6),      SurchargeTransportAC          DECIMAL(38,6),     SurchargeTransportAC_Budget   DECIMAL(38,6),     SurchargeTransportGC          DECIMAL(38,6),     SurchargeTransportGC_Budget   DECIMAL(38,6),     SurchargeTransportRC          DECIMAL(38,6),     SurchargeTransportRC_Budget   DECIMAL(38,6),     SurchargeTransportTC          DECIMAL(38,6),      CreatedETLRunId INT NOT NULL,     ModifiedETLRunId INT NOT NULL ) USING DELTA
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
