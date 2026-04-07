# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_SalesShipment.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_SalesShipment.View.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Python notebook – Databricks SQL view transpilation
# ------------------------------------------------------------
# This notebook recreates the T‑SQL view *V_SalesShipment* from the
# original script.  All table and view references are fully‑qualified
# using the placeholders `dbe_dbx_internships` and `datastore`.  Replace those
# placeholders with the actual catalog (database) and schema names
# before running the notebook on your Databricks cluster.
#
# The original T‑SQL used several non‑Spark constructs:
#   * ISNULL          → coalesce
#   * string concatenation   ( + ) → concat_ws
#   * CAST to NVARCHAR      → cast(..., 'STRING')
#   * CAST to DATE          → cast(..., 'DATE')
#   * System user variable  → current_user()
#   * ROW_NUMBER analytic   → window function in Spark
#   * NULLs from empty
#     strings      → nullif
# ------------------------------------------------------------

# ------------------------------------------------------------------
# 1.  Define the fully‑qualified view name
# ------------------------------------------------------------------
view_name = f"`{dbe_dbx_internships}`.`{datastore}`.V_SalesShipment"

# COMMAND ----------

# ------------------------------------------------------------------
# 2.  Build the CREATE VIEW statement
# ------------------------------------------------------------------
create_view_sql = f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    /* 1.  Shipping slip identifiers – use N/A if missing */
    COALESCE(CPSTS.PackingSlipId, '_N/A')                                      AS CustPackingSlipCode,

    /* 1.1  Line number – default 0 if NULL */
    COALESCE(CPSTS.LineNum, 0)                                                AS CustPackingSlipLineNumber,

    /* 1.2  Concatenated slip + line – if any part missing, use N/A */
    COALESCE(
        CONCAT_WS(' - ',
                  CPSTS.PackingSlipId,
                  CAST(CPSTS.LineNum AS STRING)
        ),
        '_N/A'
    )                                                                           AS CustPackingSlipLineNumberCombination,

    /* 2.  Sales and invoice codes */
    COALESCE(CPSTS.OrigSalesId,          '_N/A')                              AS SalesOrderCode,
    COALESCE(CITS.InvoiceId,             '_N/A')                              AS SalesInvoiceCode,

    /* 3  Company and product information  */
    COALESCE(UPPER(CPSTS.DataAreaId),    '_N/A')                              AS CompanyCode,
    COALESCE(UPPER(CPSTS.ItemId),       '_N/A')                              AS ProductCode,

    /* 4.  Customer identifiers – treat empty string as NULL, then N/A */
    COALESCE(NULLIF(SOHS.OrderingCustomerAccountNumber, ''), '_N/A')          AS OrderCustomerCode,
    COALESCE(NULLIF(SOHS.INVOICECUSTOMERACCOUNTNUMBER, ''), '_N/A')          AS CustomerCode,

    /* 5.  Inventory reference and dimension codes */
    COALESCE(CPSTS.InventRefTransId,    '_N/A')                              AS InventTransCode,
    COALESCE(CPSTS.InventDimId,        '_N/A')                              AS InventDimCode,

    /* 6.  Shipping dates – default to 1900‑01‑01 if NULL, cast as DATE */
    CAST(
        COALESCE(CPSTS.SalesLineShippingDateRequested, '1900-01-01')
        AS DATE
    )                                                                           AS RequestedShippingDate,
    CAST(
        COALESCE(CPSTS.SalesLineShippingDateConfirmed, '1900-01-01')
        AS DATE
    )                                                                           AS ConfirmedShippingDate,
    CAST(
        COALESCE(CPSTS.DeliveryDate, '1900-01-01')
        AS DATE
    )                                                                           AS ActualDeliveryDate,

    /* 7.  Unit details and quantities */
    COALESCE(UPPER(CPSTS.SalesUnit),    '_N/A')                              AS SalesUnit,
    COALESCE(CPSTS.Ordered,             0)                                   AS OrderedQuantity,
    COALESCE(CPSTS.Qty,                0)                                   AS DeliveredQuantity

FROM
    /* 8.  Primary staging table for packing slip journal */
    `dbe_dbx_internships`.`dbo`.SMRBICustPackingSlipJourStaging   AS CPSJS

    /* 9.  Lateral join with a numbered sub‑query to keep the
           first row per (DataAreaId, PackingSlipId) sorted by LineNum */
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY DataAreaId, PackingSlipId
                               ORDER BY LineNum ASC)   AS RankNr,
            *
        FROM `dbe_dbx_internships`.`dbo`.SMRBICustPackingSlipTransStaging
    ) AS CPSTS
        ON CPSJS.PackingSlipId = CPSTS.PackingSlipId
        AND CPSJS.DataAreaId   = CPSTS.DataAreaId

    /* 10.  Optional join to sales order header – left join
            because the slip may not be associated with a header */
    LEFT JOIN `dbe_dbx_internships`.`dbo`.SMRBISalesOrderHeaderStaging AS SOHS
        ON SOHS.SalesOrderNumber = CPSTS.OrigSalesId
        AND SOHS.DataAreaId      = CPSTS.DataAreaId

    /* 11.  Optional join to invoice transaction staging – left join */
    LEFT JOIN `dbe_dbx_internships`.`dbo`.SMRBICustInvoiceTransStaging AS CITS
        ON CPSTS.InvoiceTransRefRecId = CITS.CustInvoiceTransRecId
        AND CPSTS.DataAreaId          = CITS.DataAreaId

-- 12.  Optional filter: keep only the first rank for each slip
WHERE
    CPSTS.RankNr = 1
"""

# COMMAND ----------

# ------------------------------------------------------------------
# 3.  Execute the CREATE VIEW statement
# ------------------------------------------------------------------
spark.sql(create_view_sql)

# COMMAND ----------

# ------------------------------------------------------------------
# 4.  (Optional) Verify that the view was created
# ------------------------------------------------------------------
spark.sql(f"SHOW TABLES LIKE '{view_name[:view_name.rfind('.')]}'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
