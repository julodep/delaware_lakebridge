# Databricks notebook source
# MAGIC %md
# MAGIC # DWH.FactSalesInvoice.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_high/DWH.FactSalesInvoice.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the target table in the workspace catalog
# ------------------------------------------------------------
#  - All identifiers are fully‑qualified using the target catalog
#    (dbe_dbx_internships.switchschema).
#  - Data type mappings are taken from the T‑SQL definition and
#    converted to Spark SQL types.
#  - T‑SQL clauses that have no direct equivalent in Spark SQL
#    – (IDENTITY, PRIMARY KEY, FOREIGN KEY, PK/AK clustering, etc.) –
#    are omitted.  The semantics are retained where possible,
#    but enforcement is left to Databricks runtime.
#  - Mismatched collation or index settings are ignored because
#    Delta Lake does not support them directly.
#
# Note: If you need enforcement of primary key / foreign key
# constraints, you can add them as Delta Lake table properties after
# creation or implement checks in your ETL pipeline.

spark.sql("""
CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.FactSalesInvoice (
    FactSalesInvoiceId         INT      NOT NULL COMMENT 'Identity column – handled in ETL logic',
    DimSalesInvoiceId          INT      NOT NULL,
    DimSalesOrderId            INT      NOT NULL,
    DimCompanyId               INT      NOT NULL,
    DimCustomerId              INT      NOT NULL,
    DimOrderCustomerId         INT      NOT NULL,
    DimProductId               INT      NOT NULL,
    DimProductConfigurationId  INT      NOT NULL,
    DimPaymentTermsId          INT      NOT NULL,
    DimDeliveryModeId          INT      NOT NULL,
    DimDeliveryTermsId         INT      NOT NULL,
    DimTransactionCurrencyId   INT      NOT NULL,
    DimAccountingCurrencyId    INT      NOT NULL,
    DimReportingCurrencyId     INT      NOT NULL,
    DimGroupCurrencyId         INT      NOT NULL,
    DimInvoiceDateId           INT      NOT NULL,
    DimRequestedDeliveryDateId INT      NOT NULL,
    DimConfirmedDeliveryDateId INT      NOT NULL,
    DimGLAccountId             INT      NOT NULL,
    DimIntercompanyId          INT      NOT NULL,
    InvoiceMonthId             INT      NOT NULL,
    SalesInvoiceCode           STRING   NOT NULL,
    TaxWriteCode               INT      NOT NULL,
    SalesOrderStatus           STRING   NOT NULL,
    InventTransCode            STRING   NOT NULL,
    InventDimCode              STRING   NOT NULL,
    SalesInvoiceLineNumber     STRING   NOT NULL,
    SalesInvoiceLineNumberCombination STRING  NOT NULL,
    SalesUnit                  STRING   NOT NULL,

    InvoicedQuantity_InventoryUnit DECIMAL(38,6)  NULL,
    InvoicedQuantity_PurchaseUnit  DECIMAL(38,6)  NULL,
    InvoicedQuantity_SalesUnit     DECIMAL(38,6)  NULL,

    SalesPricePerUnitTC        DECIMAL(38,6) NOT NULL,
    SalesPricePerUnitAC        DECIMAL(38,6) NOT NULL,
    SalesPricePerUnitRC        DECIMAL(38,6) NOT NULL,
    SalesPricePerUnitGC        DECIMAL(38,6) NOT NULL,
    SalesPricePerUnitAC_Budget  DECIMAL(38,6) NOT NULL,
    SalesPricePerUnitRC_Budget  DECIMAL(38,6) NOT NULL,
    SalesPricePerUnitGC_Budget  DECIMAL(38,6) NOT NULL,

    GrossSalesTC      DECIMAL(38,6) NOT NULL,
    GrossSalesAC      DECIMAL(38,6) NOT NULL,
    GrossSalesRC      DECIMAL(38,6) NOT NULL,
    GrossSalesGC      DECIMAL(38,6) NOT NULL,
    GrossSalesAC_Budget DECIMAL(38,6) NOT NULL,
    GrossSalesRC_Budget DECIMAL(38,6) NOT NULL,
    GrossSalesGC_Budget DECIMAL(38,6) NOT NULL,

    DiscountAmountTC      DECIMAL(38,6) NOT NULL,
    DiscountAmountAC      DECIMAL(38,6) NOT NULL,
    DiscountAmountRC      DECIMAL(38,6) NOT NULL,
    DiscountAmountGC      DECIMAL(38,6) NOT NULL,
    DiscountAmountAC_Budget DECIMAL(38,6) NOT NULL,
    DiscountAmountRC_Budget DECIMAL(38,6) NOT NULL,
    DiscountAmountGC_Budget DECIMAL(38,6) NOT NULL,

    InvoicedSalesAmountTC        DECIMAL(32,17) NOT NULL,
    InvoicedSalesAmountAC        DECIMAL(38,6)  NOT NULL,
    InvoicedSalesAmountRC        DECIMAL(38,6)  NOT NULL,
    InvoicedSalesAmountGC        DECIMAL(38,6)  NOT NULL,
    InvoicedSalesAmountAC_Budget DECIMAL(38,6) NOT NULL,
    InvoicedSalesAmountRC_Budget DECIMAL(38,6) NOT NULL,
    InvoicedSalesAmountGC_Budget DECIMAL(38,6) NOT NULL,

    NetSalesAmountTC     DECIMAL(38,6) NOT NULL,
    NetSalesAmountAC     DECIMAL(38,6) NOT NULL,
    NetSalesAmountRC     DECIMAL(38,6) NOT NULL,
    NetSalesAmountGC     DECIMAL(38,6) NOT NULL,
    NetSalesAmountAC_Budget DECIMAL(38,6) NOT NULL,
    NetSalesAmountRC_Budget DECIMAL(38,6) NOT NULL,
    NetSalesAmountGC_Budget DECIMAL(38,6) NOT NULL,

    AppliedExchangeRateTC   DECIMAL(38,6) NOT NULL,
    AppliedExchangeRateAC   DECIMAL(38,20) NOT NULL,
    AppliedExchangeRateRC   DECIMAL(38,20) NOT NULL,
    AppliedExchangeRateGC   DECIMAL(38,20) NOT NULL,
    AppliedExchangeRateAC_Budget   DECIMAL(38,20) NOT NULL,
    AppliedExchangeRateRC_Budget   DECIMAL(38,20) NOT NULL,
    AppliedExchangeRateGC_Budget   DECIMAL(38,20) NOT NULL,

    CostOfGoodsSoldTC        DECIMAL(38,7) NOT NULL,
    CostOfGoodsSoldAC        DECIMAL(38,6) NOT NULL,
    CostOfGoodsSoldRC        DECIMAL(38,6) NOT NULL,
    CostOfGoodsSoldGC        DECIMAL(38,6) NOT NULL,
    CostOfGoodsSoldAC_Budget DECIMAL(38,6) NOT NULL,
    CostOfGoodsSoldRC_Budget DECIMAL(38,6) NOT NULL,
    CostOfGoodsSoldGC_Budget DECIMAL(38,6) NOT NULL,

    GrossMarginTC   DECIMAL(38,6) NOT NULL,
    GrossMarginAC   DECIMAL(38,6) NOT NULL,
    GrossMarginRC   DECIMAL(38,6) NOT NULL,
    GrossMarginGC   DECIMAL(38,6) NOT NULL,
    GrossMarginAC_Budget DECIMAL(38,6) NOT NULL,
    GrossMarginRC_Budget DECIMAL(38,6) NOT NULL,
    GrossMarginGC_Budget DECIMAL(38,6) NOT NULL,

    InvoicedSalesAmountInclTaxAC DECIMAL(38,6) NULL,
    InvoicedSalesAmountInclTaxGC DECIMAL(38,6) NULL,
    InvoicedSalesAmountInclTaxRC DECIMAL(38,6) NULL,
    InvoicedSalesAmountInclTaxTC DECIMAL(38,6) NULL,

    RebateAmountCancelledAC  DECIMAL(38,6) NULL,
    RebateAmountCancelledAC_Budget DECIMAL(38,6) NULL,
    RebateAmountCancelledGC  DECIMAL(38,6) NULL,
    RebateAmountCancelledGC_Budget DECIMAL(38,6) NULL,
    RebateAmountCancelledRC  DECIMAL(38,6) NULL,
    RebateAmountCancelledRC_Budget DECIMAL(38,6) NULL,
    RebateAmountCancelledTC  DECIMAL(38,6) NULL,

    RebateAmountCompletedAC  DECIMAL(38,6) NULL,
    RebateAmountCompletedAC_Budget DECIMAL(38,6) NULL,
    RebateAmountCompletedGC  DECIMAL(38,6) NULL,
    RebateAmountCompletedGC_Budget DECIMAL(38,6) NULL,
    RebateAmountCompletedRC  DECIMAL(38,6) NULL,
    RebateAmountCompletedRC_Budget DECIMAL(38,6) NULL,
    RebateAmountCompletedTC  DECIMAL(38,6) NULL,

    RebateAmountMarkedAC  DECIMAL(38,6) NULL,
    RebateAmountMarkedAC_Budget DECIMAL(38,6) NULL,
    RebateAmountMarkedGC  DECIMAL(38,6) NULL,
    RebateAmountMarkedGC_Budget DECIMAL(38,6) NULL,
    RebateAmountMarkedRC  DECIMAL(38,6) NULL,
    RebateAmountMarkedRC_Budget DECIMAL(38,6) NULL,
    RebateAmountMarkedTC  DECIMAL(38,6) NULL,

    RebateAmountOriginalAC  DECIMAL(38,6) NULL,
    RebateAmountOriginalAC_Budget DECIMAL(38,6) NULL,
    RebateAmountOriginalGC  DECIMAL(38,6) NULL,
    RebateAmountOriginalGC_Budget DECIMAL(38,6) NULL,
    RebateAmountOriginalRC  DECIMAL(38,6) NULL,
    RebateAmountOriginalRC_Budget DECIMAL(38,6) NULL,
    RebateAmountOriginalTC  DECIMAL(38,6) NULL,

    RebateAmountVarianceAC  DECIMAL(38,6) NULL,
    RebateAmountVarianceAC_Budget DECIMAL(38,6) NULL,
    RebateAmountVarianceGC  DECIMAL(38,6) NULL,
    RebateAmountVarianceGC_Budget DECIMAL(38,6) NULL,
    RebateAmountVarianceRC  DECIMAL(38,6) NULL,
    RebateAmountVarianceRC_Budget DECIMAL(38,6) NULL,
    RebateAmountVarianceTC  DECIMAL(38,6) NULL,

    SurchargeTotalAC        DECIMAL(38,6) NULL,
    SurchargeTotalAC_Budget DECIMAL(38,6) NULL,
    SurchargeTotalGC        DECIMAL(38,6) NULL,
    SurchargeTotalGC_Budget DECIMAL(38,6) NULL,
    SurchargeTotalRC        DECIMAL(38,6) NULL,
    SurchargeTotalRC_Budget DECIMAL(38,6) NULL,
    SurchargeTotalTC        DECIMAL(38,6) NULL,

    CreatedETLRunId      INT NOT NULL,
    ModifiedETLRunId    INT NOT NULL,
    DimBusinessSegmentId INT NOT NULL,
    DimDepartmentId     INT NOT NULL,
    DimEndCustomerId    INT NOT NULL,
    DimLocationId       INT NOT NULL,
    DimShipmentContractId INT NOT NULL,
    DimLocalAccountId  INT NOT NULL,
    DimProductFDId     INT NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'NULL': extra input 'NULL'. SQLSTATE: 42601 (line 1, pos 1640)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE dbe_dbx_internships.switchschema.FactSalesInvoice (     FactSalesInvoiceId         INT      NOT NULL COMMENT 'Identity column – handled in ETL logic',     DimSalesInvoiceId          INT      NOT NULL,     DimSalesOrderId            INT      NOT NULL,     DimCompanyId               INT      NOT NULL,     DimCustomerId              INT      NOT NULL,     DimOrderCustomerId         INT      NOT NULL,     DimProductId               INT      NOT NULL,     DimProductConfigurationId  INT      NOT NULL,     DimPaymentTermsId          INT      NOT NULL,     DimDeliveryModeId          INT      NOT NULL,     DimDeliveryTermsId         INT      NOT NULL,     DimTransactionCurrencyId   INT      NOT NULL,     DimAccountingCurrencyId    INT      NOT NULL,     DimReportingCurrencyId     INT      NOT NULL,     DimGroupCurrencyId         INT      NOT NULL,     DimInvoiceDateId           INT      NOT NULL,     DimRequestedDeliveryDateId INT      NOT NULL,     DimConfirmedDeliveryDateId INT      NOT NULL,     DimGLAccountId             INT      NOT NULL,     DimIntercompanyId          INT      NOT NULL,     InvoiceMonthId             INT      NOT NULL,     SalesInvoiceCode           STRING   NOT NULL,     TaxWriteCode               INT      NOT NULL,     SalesOrderStatus           STRING   NOT NULL,     InventTransCode            STRING   NOT NULL,     InventDimCode              STRING   NOT NULL,     SalesInvoiceLineNumber     STRING   NOT NULL,     SalesInvoiceLineNumberCombination STRING  NOT NULL,     SalesUnit                  STRING   NOT NULL,      InvoicedQuantity_InventoryUnit DECIMAL(38,6)  NULL,     InvoicedQuantity_PurchaseUnit  DECIMAL(38,6)  NULL,     InvoicedQuantity_SalesUnit     DECIMAL(38,6)  NULL,      SalesPricePerUnitTC        DECIMAL(38,6) NOT NULL,     SalesPricePerUnitAC        DECIMAL(38,6) NOT NULL,     SalesPricePerUnitRC        DECIMAL(38,6) NOT NULL,     SalesPricePerUnitGC        DECIMAL(38,6) NOT NULL,     SalesPricePerUnitAC_Budget  DECIMAL(38,6) NOT NULL,     SalesPricePerUnitRC_Budget  DECIMAL(38,6) NOT NULL,     SalesPricePerUnitGC_Budget  DECIMAL(38,6) NOT NULL,      GrossSalesTC      DECIMAL(38,6) NOT NULL,     GrossSalesAC      DECIMAL(38,6) NOT NULL,     GrossSalesRC      DECIMAL(38,6) NOT NULL,     GrossSalesGC      DECIMAL(38,6) NOT NULL,     GrossSalesAC_Budget DECIMAL(38,6) NOT NULL,     GrossSalesRC_Budget DECIMAL(38,6) NOT NULL,     GrossSalesGC_Budget DECIMAL(38,6) NOT NULL,      DiscountAmountTC      DECIMAL(38,6) NOT NULL,     DiscountAmountAC      DECIMAL(38,6) NOT NULL,     DiscountAmountRC      DECIMAL(38,6) NOT NULL,     DiscountAmountGC      DECIMAL(38,6) NOT NULL,     DiscountAmountAC_Budget DECIMAL(38,6) NOT NULL,     DiscountAmountRC_Budget DECIMAL(38,6) NOT NULL,     DiscountAmountGC_Budget DECIMAL(38,6) NOT NULL,      InvoicedSalesAmountTC        DECIMAL(32,17) NOT NULL,     InvoicedSalesAmountAC        DECIMAL(38,6)  NOT NULL,     InvoicedSalesAmountRC        DECIMAL(38,6)  NOT NULL,     InvoicedSalesAmountGC        DECIMAL(38,6)  NOT NULL,     InvoicedSalesAmountAC_Budget DECIMAL(38,6) NOT NULL,     InvoicedSalesAmountRC_Budget DECIMAL(38,6) NOT NULL,     InvoicedSalesAmountGC_Budget DECIMAL(38,6) NOT NULL,      NetSalesAmountTC     DECIMAL(38,6) NOT NULL,     NetSalesAmountAC     DECIMAL(38,6) NOT NULL,     NetSalesAmountRC     DECIMAL(38,6) NOT NULL,     NetSalesAmountGC     DECIMAL(38,6) NOT NULL,     NetSalesAmountAC_Budget DECIMAL(38,6) NOT NULL,     NetSalesAmountRC_Budget DECIMAL(38,6) NOT NULL,     NetSalesAmountGC_Budget DECIMAL(38,6) NOT NULL,      AppliedExchangeRateTC   DECIMAL(38,6) NOT NULL,     AppliedExchangeRateAC   DECIMAL(38,20) NOT NULL,     AppliedExchangeRateRC   DECIMAL(38,20) NOT NULL,     AppliedExchangeRateGC   DECIMAL(38,20) NOT NULL,     AppliedExchangeRateAC_Budget   DECIMAL(38,20) NOT NULL,     AppliedExchangeRateRC_Budget   DECIMAL(38,20) NOT NULL,     AppliedExchangeRateGC_Budget   DECIMAL(38,20) NOT NULL,      CostOfGoodsSoldTC        DECIMAL(38,7) NOT NULL,     CostOfGoodsSoldAC        DECIMAL(38,6) NOT NULL,     CostOfGoodsSoldRC        DECIMAL(38,6) NOT NULL,     CostOfGoodsSoldGC        DECIMAL(38,6) NOT NULL,     CostOfGoodsSoldAC_Budget DECIMAL(38,6) NOT NULL,     CostOfGoodsSoldRC_Budget DECIMAL(38,6) NOT NULL,     CostOfGoodsSoldGC_Budget DECIMAL(38,6) NOT NULL,      GrossMarginTC   DECIMAL(38,6) NOT NULL,     GrossMarginAC   DECIMAL(38,6) NOT NULL,     GrossMarginRC   DECIMAL(38,6) NOT NULL,     GrossMarginGC   DECIMAL(38,6) NOT NULL,     GrossMarginAC_Budget DECIMAL(38,6) NOT NULL,     GrossMarginRC_Budget DECIMAL(38,6) NOT NULL,     GrossMarginGC_Budget DECIMAL(38,6) NOT NULL,      InvoicedSalesAmountInclTaxAC DECIMAL(38,6) NULL,     InvoicedSalesAmountInclTaxGC DECIMAL(38,6) NULL,     InvoicedSalesAmountInclTaxRC DECIMAL(38,6) NULL,     InvoicedSalesAmountInclTaxTC DECIMAL(38,6) NULL,      RebateAmountCancelledAC  DECIMAL(38,6) NULL,     RebateAmountCancelledAC_Budget DECIMAL(38,6) NULL,     RebateAmountCancelledGC  DECIMAL(38,6) NULL,     RebateAmountCancelledGC_Budget DECIMAL(38,6) NULL,     RebateAmountCancelledRC  DECIMAL(38,6) NULL,     RebateAmountCancelledRC_Budget DECIMAL(38,6) NULL,     RebateAmountCancelledTC  DECIMAL(38,6) NULL,      RebateAmountCompletedAC  DECIMAL(38,6) NULL,     RebateAmountCompletedAC_Budget DECIMAL(38,6) NULL,     RebateAmountCompletedGC  DECIMAL(38,6) NULL,     RebateAmountCompletedGC_Budget DECIMAL(38,6) NULL,     RebateAmountCompletedRC  DECIMAL(38,6) NULL,     RebateAmountCompletedRC_Budget DECIMAL(38,6) NULL,     RebateAmountCompletedTC  DECIMAL(38,6) NULL,      RebateAmountMarkedAC  DECIMAL(38,6) NULL,     RebateAmountMarkedAC_Budget DECIMAL(38,6) NULL,     RebateAmountMarkedGC  DECIMAL(38,6) NULL,     RebateAmountMarkedGC_Budget DECIMAL(38,6) NULL,     RebateAmountMarkedRC  DECIMAL(38,6) NULL,     RebateAmountMarkedRC_Budget DECIMAL(38,6) NULL,     RebateAmountMarkedTC  DECIMAL(38,6) NULL,      RebateAmountOriginalAC  DECIMAL(38,6) NULL,     RebateAmountOriginalAC_Budget DECIMAL(38,6) NULL,     RebateAmountOriginalGC  DECIMAL(38,6) NULL,     RebateAmountOriginalGC_Budget DECIMAL(38,6) NULL,     RebateAmountOriginalRC  DECIMAL(38,6) NULL,     RebateAmountOriginalRC_Budget DECIMAL(38,6) NULL,     RebateAmountOriginalTC  DECIMAL(38,6) NULL,      RebateAmountVarianceAC  DECIMAL(38,6) NULL,     RebateAmountVarianceAC_Budget DECIMAL(38,6) NULL,     RebateAmountVarianceGC  DECIMAL(38,6) NULL,     RebateAmountVarianceGC_Budget DECIMAL(38,6) NULL,     RebateAmountVarianceRC  DECIMAL(38,6) NULL,     RebateAmountVarianceRC_Budget DECIMAL(38,6) NULL,     RebateAmountVarianceTC  DECIMAL(38,6) NULL,      SurchargeTotalAC        DECIMAL(38,6) NULL,     SurchargeTotalAC_Budget DECIMAL(38,6) NULL,     SurchargeTotalGC        DECIMAL(38,6) NULL,     SurchargeTotalGC_Budget DECIMAL(38,6) NULL,     SurchargeTotalRC        DECIMAL(38,6) NULL,     SurchargeTotalRC_Budget DECIMAL(38,6) NULL,     SurchargeTotalTC        DECIMAL(38,6) NULL,      CreatedETLRunId      INT NOT NULL,     ModifiedETLRunId    INT NOT NULL,     DimBusinessSegmentId INT NOT NULL,     DimDepartmentId     INT NOT NULL,     DimEndCustomerId    INT NOT NULL,     DimLocationId       INT NOT NULL,     DimShipmentContractId INT NOT NULL,     DimLocalAccountId  INT NOT NULL,     DimProductFDId     INT NOT NULL );
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
