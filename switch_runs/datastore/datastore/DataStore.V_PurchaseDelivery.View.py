# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PurchaseDelivery.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PurchaseDelivery.View.sql`

# COMMAND ----------

# --------------------------------------------------------------
#  View: V_PurchaseDelivery – converted from T‑SQL to Databricks SQL
#  Target catalog: dbe_dbx_internships
#  Target schema : datastore
#
#  All object references are fully qualified:
#       dbe_dbx_internships.datastore.{object_name}
# --------------------------------------------------------------

from pyspark.sql import SparkSession

# COMMAND ----------

# create Spark session (assumes Databricks runtime)
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ------------------------------------------------------------------
#  1.  Build the statement as a string and then execute it with
#      spark.sql().  In Databricks the CREATE VIEW syntax is identical
#      to standard Spark SQL.
#
#  2.  Replace the T‑SQL functions that have no direct equivalent:
#          ISNULL  ->  COALESCE
#          NULLIF  ->  NULLIF (Spark SQL supports it)
#          CAST(ISNULL(..., '1900‑01‑01') AS DATE)
#                 ->  CAST(COALESCE(..., '1900‑01‑01') AS DATE)
#          NVARCHAR(...)          ->  CAST(... AS STRING)
#
#  3.  All identifiers are converted from bracketed notation
#      [Schema].[Table] to dot‑separated notation with the
#      fully‑qualified catalog and schema.
# ------------------------------------------------------------------

view_sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_PurchaseDelivery` AS
SELECT
    -- Packing slip identifier
    VPSJS.PackingSlipId AS PackingSlipCode,

    -- Purchase order code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.PurchId, ''), '_N/A') AS PurchaseOrderCode,

    -- Company code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.DataAreaId, ''), '_N/A') AS CompanyCode,

    -- Product configuration code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSTS.InventDimId, ''), '_N/A') AS ProductConfigurationCode,

    -- Product code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSTS.ItemId, ''), '_N/A') AS ProductCode,

    -- Order supplier code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.OrderAccount, ''), '_N/A') AS OrderSupplierCode,

    -- Supplier code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.InvoiceAccount, ''), '_N/A') AS SupplierCode,

    -- Delivery mode code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.DlvMode, ''), '_N/A') AS DeliveryModeCode,

    -- Delivery terms code, replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.DlvTerm, ''), '_N/A') AS DeliveryTermsCode,

    -- Actual delivery date – if NULL, default to 1900‑01‑01 then cast to DATE
    CAST(COALESCE(VPSJS.DeliveryDate, '1900-01-01') AS date) AS ActualDeliveryDate,

    -- Requested delivery date – if NULL, default to 1900‑01‑01 then cast to DATE
    CAST(COALESCE(PO.RequestedDeliveryDate, '1900-01-01') AS date) AS RequestedDeliveryDate,

    -- Confirmed delivery date – if NULL, default to 1900‑01‑01 then cast to DATE
    CAST(COALESCE(PO.ConfirmedDeliveryDate, '1900-01-01') AS date) AS ConfirmedDeliveryDate,

    -- Purchase type – cast to STRING and replace empty string with '_N/A'
    CAST(COALESCE(NULLIF(StringMapPurchaseType.Name, ''), '_N/A') AS STRING) AS PurchaseType,

    -- Purchase order line number – NULL → -1
    COALESCE(VPSTS.PurchaseLineLineNumber, -1) AS PurchaseOrderLineNumber,

    -- Delivery name – replace empty string with '_N/A'
    COALESCE(NULLIF(VPSJS.DeliveryName, ''), '_N/A') AS DeliveryName,

    -- Delivery line number – NULL → -1
    COALESCE(VPSTS.LineNum, -1) AS DeliveryLineNumber,

    -- Purchase unit – replace empty string with '_N/A'
    COALESCE(NULLIF(VPSTS.PurchUnit, ''), '_N/A') AS PurchaseUnit,

    -- Quantities
    VPSTS.Ordered  AS QuantityOrdered,
    VPSTS.Qty      AS QuantityDelivered
FROM
    `dbe_dbx_internships`.`datastore`.`SMRBIVendPackingSlipJourStaging` AS VPSJS
LEFT JOIN
    `dbe_dbx_internships`.`datastore`.`SMRBIVendPackingSlipTransStaging` AS VPSTS
    ON VPSJS.VendPackingSlipJourRecId = VPSTS.VendPackingSlipJour
   AND VPSJS.DataAreaId = VPSTS.DataAreaId
LEFT JOIN
    `dbe_dbx_internships`.`datastore`.`PurchaseOrder` AS PO
    ON VPSJS.PurchId = PO.PurchaseOrderCode
   AND VPSTS.PurchaseLineLineNumber = PO.PurchaseOrderLineNumber
   AND VPSTS.DataAreaId = PO.CompanyCode
LEFT JOIN
    `dbe_dbx_internships`.`datastore`.`StringMap` AS StringMapPurchaseType
    ON StringMapPurchaseType.SourceTable = 'PurchaseType'
   AND StringMapPurchaseType.Enum = CAST(VPSJS.PurchaseType AS STRING)
"""

# COMMAND ----------

# Execute the statement
spark.sql(view_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 3183)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_PurchaseDelivery` AS SELECT     -- Packing slip identifier     VPSJS.PackingSlipId AS PackingSlipCode,      -- Purchase order code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.PurchId, ''), '_N/A') AS PurchaseOrderCode,      -- Company code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.DataAreaId, ''), '_N/A') AS CompanyCode,      -- Product configuration code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSTS.InventDimId, ''), '_N/A') AS ProductConfigurationCode,      -- Product code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSTS.ItemId, ''), '_N/A') AS ProductCode,      -- Order supplier code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.OrderAccount, ''), '_N/A') AS OrderSupplierCode,      -- Supplier code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.InvoiceAccount, ''), '_N/A') AS SupplierCode,      -- Delivery mode code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.DlvMode, ''), '_N/A') AS DeliveryModeCode,      -- Delivery terms code, replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.DlvTerm, ''), '_N/A') AS DeliveryTermsCode,      -- Actual delivery date – if NULL, default to 1900‑01‑01 then cast to DATE     CAST(COALESCE(VPSJS.DeliveryDate, '1900-01-01') AS date) AS ActualDeliveryDate,      -- Requested delivery date – if NULL, default to 1900‑01‑01 then cast to DATE     CAST(COALESCE(PO.RequestedDeliveryDate, '1900-01-01') AS date) AS RequestedDeliveryDate,      -- Confirmed delivery date – if NULL, default to 1900‑01‑01 then cast to DATE     CAST(COALESCE(PO.ConfirmedDeliveryDate, '1900-01-01') AS date) AS ConfirmedDeliveryDate,      -- Purchase type – cast to STRING and replace empty string with '_N/A'     CAST(COALESCE(NULLIF(StringMapPurchaseType.Name, ''), '_N/A') AS STRING) AS PurchaseType,      -- Purchase order line number – NULL → -1     COALESCE(VPSTS.PurchaseLineLineNumber, -1) AS PurchaseOrderLineNumber,      -- Delivery name – replace empty string with '_N/A'     COALESCE(NULLIF(VPSJS.DeliveryName, ''), '_N/A') AS DeliveryName,      -- Delivery line number – NULL → -1     COALESCE(VPSTS.LineNum, -1) AS DeliveryLineNumber,      -- Purchase unit – replace empty string with '_N/A'     COALESCE(NULLIF(VPSTS.PurchUnit, ''), '_N/A') AS PurchaseUnit,      -- Quantities     VPSTS.Ordered  AS QuantityOrdered,     VPSTS.Qty      AS QuantityDelivered FROM     `_placeholder_`.`_placeholder_`.`SMRBIVendPackingSlipJourStaging` AS VPSJS LEFT JOIN     `_placeholder_`.`_placeholder_`.`SMRBIVendPackingSlipTransStaging` AS VPSTS     ON VPSJS.VendPackingSlipJourRecId = VPSTS.VendPackingSlipJour    AND VPSJS.DataAreaId = VPSTS.DataAreaId LEFT JOIN     `_placeholder_`.`_placeholder_`.`PurchaseOrder` AS PO     ON VPSJS.PurchId = PO.PurchaseOrderCode    AND VPSTS.PurchaseLineLineNumber = PO.PurchaseOrderLineNumber    AND VPSTS.DataAreaId = PO.CompanyCode LEFT JOIN     `_placeholder_`.`_placeholder_`.`StringMap` AS StringMapPurchaseType     ON StringMapPurchaseType.SourceTable = 'PurchaseType'    AND StringMapPurchaseType.Enum = CAST(VPSJS.PurchaseType AS STRING)
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
