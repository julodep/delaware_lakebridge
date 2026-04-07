# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_NetRequirements.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_NetRequirements.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1.  Transpile T-SQL view into a Databricks Unity Catalog view
# ------------------------------------------------------------------
#  * KEEP ALL table names fully‑qualified:  `dbe_dbx_internships.datastore.{table}`
#  * Replace SQL Server‑only constructs with Spark SQL equivalents
#  * Use a CTE to compute the windowed ROW_NUMBER() and then project
#    the final columns
#  * We deliberately *do NOT* use `DROP VIEW IF EXISTS` – it will
#    overwrite any existing view with the same name.
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# 2.  Build the CREATE VIEW statement
# ------------------------------------------------------------------
view_sql = f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_NetRequirements` AS
WITH base AS (
    SELECT
        -- Primary keys -----------------------------------------------------------
        RTS.ReqTransReqId                                 AS RecId,

        -- Company / reference & metadata -----------------------------------------
        COALESCE(NULLIF(RTS.DataAreaId, ''), '_N/A')      AS CompanyCode,
        COALESCE(SM1.Name, '_N/A')                        AS ReferenceType,
        COALESCE(RPVS.ReqPlanId, '_N/A')                  AS PlanVersion,

        COALESCE(NULLIF(UPPER(RTS.ItemId), ''), '_N/A')   AS ProductCode,
        COALESCE(NULLIF(RTS.CovInventDimId, ''), '_N/A')  AS InventDimCode,

        RTS.RefType                                       AS RefType,

        -- Dates ---------------------------------------------------------------
        CASE
            WHEN COALESCE(RTS.ReqDate, DATE '1900-01-01') < DATE '2005-01-01'
                THEN DATE '1900-01-01'
            ELSE COALESCE(RTS.ReqDate, DATE '1900-01-01')
        END                                               AS RequirementDate,

        TIMESTAMP '1900-01-01 00:00:00.000'              AS RequirementTime,

        CASE
            WHEN COALESCE(RTS.ReqDate, DATE '1900-01-01') < DATE '2005-01-01'
                THEN DATE '1900-01-01'
            ELSE RTS.ReqDate
        END                                               AS RequirementDateTime,

        -- Reference / product details ------------------------------------------
        COALESCE(NULLIF(RTS.RefId, ''), '_N/A')           AS ReferenceCode,
        COALESCE(
            NULLIF(UPPER(PTS.ItemId), ''),
            NULLIF(UPPER(RPO.ItemId), ''),
            '_N/A'
        )                                                   AS ProducedItemCode,

        COALESCE(NULLIF(SOHS.OrderingCustomerAccountNumber, ''), '_N/A')
                                                      AS CustomerCode,

        COALESCE(NULLIF(POHS.OrderVendorAccountNumber, ''), '_N/A')
                                                      AS VendorCode,

        -- Action / futures ----------------------------------------------
        NULLIF(RTS.ActionDate, '')                       AS ActionDate,

        COALESCE(NULLIF(RTS.ActionDays, ''), 0)          AS ActionDays,

        COALESCE(SM3.Name, '_N/A')                      AS ActionType,
        COALESCE(SM4.Name, '_N/A')                      AS ActionMarked,

        COALESCE(NULLIF(RTS.FuturesDate, ''), '')        AS FuturesDate,
        COALESCE(NULLIF(RTS.FuturesDays, ''), 0)         AS FuturesDays,

        COALESCE(SM6.Name, '_N/A')                      AS FuturesCalculated,
        COALESCE(SM5.Name, '_N/A')                      AS FuturesMarked,

        COALESCE(SM2.Name, '_N/A')                      AS Direction,

        -- Windowed rank ---------------------------------------------
        ROW_NUMBER() OVER (
            PARTITION BY
                RTS.DataAreaId,
                RTS.CovInventDimId,
                RPVS.ReqPlanId,
                RTS.ItemId
            ORDER BY
                RTS.ReqDate ASC,
                RTS.Direction DESC
        )                                                AS _rn,

        -- Quantities ---------------------------------------------
        COALESCE(RTS.Qty, 0)                             AS Quantity

    FROM `dbe_dbx_internships`.`datastore`.`SMRBIReqTransStaging`  RTS
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIReqPlanVersionStaging` RPVS
        ON RTS.DataAreaId   = RPVS.ReqPlanDataAreaId
       AND RTS.PlanVersion = RPVS.ReqPlanVersionRecId

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM1
        ON SM1.SourceTable     = 'SMRBIReqTransStaging'
       AND SM1.SourceColumn   = 'ReqRefType'
       AND SM1.Enum           = RTS.RefType

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM2
        ON SM2.SourceTable     = 'SMRBIReqTransStaging'
       AND SM2.SourceColumn   = 'Direction'
       AND SM2.Enum           = RTS.Direction

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM3
        ON SM3.SourceTable     = 'SMRBIReqTransStaging'
       AND SM3.SourceColumn   = 'ActionType'
       AND SM3.Enum           = RTS.ActionType

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM4
        ON SM4.SourceTable     = 'SMRBIReqTransStaging'
       AND SM4.SourceColumn   = 'ActionMarked'
       AND SM4.Enum           = RTS.ActionMarked

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM5
        ON SM5.SourceTable     = 'SMRBIReqTransStaging'
       AND SM5.SourceColumn   = 'FuturesMarked'
       AND SM5.Enum           = RTS.FuturesMarked

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM6
        ON SM6.SourceTable     = 'SMRBIReqTransStaging'
       AND SM6.SourceColumn   = 'FuturesCalculated'
       AND SM6.Enum           = RTS.FuturesCalculated

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIProdTableStaging` PTS
        ON RTS.RefId   = PTS.ProdId
       AND RTS.DataAreaId = PTS.COMPANY

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIPurchPurchaseOrderHeaderStaging` POHS
        ON RTS.RefId   = POHS.PurchaseOrderNumber
       AND RTS.DataAreaId = POHS.DataAreaId

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBISalesOrderHeaderStaging` SOHS
        ON RTS.RefId   = SOHS.SalesOrderNumber
       AND RTS.DataAreaId = SOHS.DataAreaId

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIReqPOStaging` RPO
        ON RTS.RefId      = RPO.RefId
       AND RTS.DataAreaId = RPO.DataAreaId
       AND RTS.PlanVersion = RPO.PlanVersion
)
SELECT
    RecId,
    CompanyCode,
    ReferenceType,
    PlanVersion,
    ProductCode,
    InventDimCode,
    RequirementDate,
    RequirementTime,
    RequirementDateTime,
    ReferenceCode,
    ProducedItemCode,
    CustomerCode,
    VendorCode,
    ActionDate,
    ActionDays,
    ActionType,
    ActionMarked,
    FuturesDate,
    FuturesDays,
    FuturesCalculated,
    FuturesMarked,
    Direction,
    CASE
        WHEN Base.RefType = 14 THEN 0
        ELSE Base._rn
    END                                                AS RankNr,
    Quantity,
    CASE
        WHEN Base.RefType IN (31, 32, 33, 34, 43) THEN 0
        ELSE Base.Quantity
    END                                                AS QuantityConfirmed
FROM base AS Base
"""

# COMMAND ----------

# ------------------------------------------------------------------
# 3.  Execute the CREATE VIEW
# ------------------------------------------------------------------
spark.sql(view_sql)

# COMMAND ----------

# ------------------------------------------------------------------
# 4.  Verify that the view has been created
# ------------------------------------------------------------------
# (Optional) Show a few rows
spark.sql(f"SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_NetRequirements` LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 6382)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_NetRequirements` AS WITH base AS (     SELECT         -- Primary keys -----------------------------------------------------------         RTS.ReqTransReqId                                 AS RecId,          -- Company / reference & metadata -----------------------------------------         COALESCE(NULLIF(RTS.DataAreaId, ''), '_N/A')      AS CompanyCode,         COALESCE(SM1.Name, '_N/A')                        AS ReferenceType,         COALESCE(RPVS.ReqPlanId, '_N/A')                  AS PlanVersion,          COALESCE(NULLIF(UPPER(RTS.ItemId), ''), '_N/A')   AS ProductCode,         COALESCE(NULLIF(RTS.CovInventDimId, ''), '_N/A')  AS InventDimCode,          RTS.RefType                                       AS RefType,          -- Dates ---------------------------------------------------------------         CASE             WHEN COALESCE(RTS.ReqDate, DATE '1900-01-01') < DATE '2005-01-01'                 THEN DATE '1900-01-01'             ELSE COALESCE(RTS.ReqDate, DATE '1900-01-01')         END                                               AS RequirementDate,          TIMESTAMP '1900-01-01 00:00:00.000'              AS RequirementTime,          CASE             WHEN COALESCE(RTS.ReqDate, DATE '1900-01-01') < DATE '2005-01-01'                 THEN DATE '1900-01-01'             ELSE RTS.ReqDate         END                                               AS RequirementDateTime,          -- Reference / product details ------------------------------------------         COALESCE(NULLIF(RTS.RefId, ''), '_N/A')           AS ReferenceCode,         COALESCE(             NULLIF(UPPER(PTS.ItemId), ''),             NULLIF(UPPER(RPO.ItemId), ''),             '_N/A'         )                                                   AS ProducedItemCode,          COALESCE(NULLIF(SOHS.OrderingCustomerAccountNumber, ''), '_N/A')                                                       AS CustomerCode,          COALESCE(NULLIF(POHS.OrderVendorAccountNumber, ''), '_N/A')                                                       AS VendorCode,          -- Action / futures ----------------------------------------------         NULLIF(RTS.ActionDate, '')                       AS ActionDate,          COALESCE(NULLIF(RTS.ActionDays, ''), 0)          AS ActionDays,          COALESCE(SM3.Name, '_N/A')                      AS ActionType,         COALESCE(SM4.Name, '_N/A')                      AS ActionMarked,          COALESCE(NULLIF(RTS.FuturesDate, ''), '')        AS FuturesDate,         COALESCE(NULLIF(RTS.FuturesDays, ''), 0)         AS FuturesDays,          COALESCE(SM6.Name, '_N/A')                      AS FuturesCalculated,         COALESCE(SM5.Name, '_N/A')                      AS FuturesMarked,          COALESCE(SM2.Name, '_N/A')                      AS Direction,          -- Windowed rank ---------------------------------------------         ROW_NUMBER() OVER (             PARTITION BY                 RTS.DataAreaId,                 RTS.CovInventDimId,                 RPVS.ReqPlanId,                 RTS.ItemId             ORDER BY                 RTS.ReqDate ASC,                 RTS.Direction DESC         )                                                AS _rn,          -- Quantities ---------------------------------------------         COALESCE(RTS.Qty, 0)                             AS Quantity      FROM `_placeholder_`.`_placeholder_`.`SMRBIReqTransStaging`  RTS     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIReqPlanVersionStaging` RPVS         ON RTS.DataAreaId   = RPVS.ReqPlanDataAreaId        AND RTS.PlanVersion = RPVS.ReqPlanVersionRecId      LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` SM1         ON SM1.SourceTable     = 'SMRBIReqTransStaging'        AND SM1.SourceColumn   = 'ReqRefType'        AND SM1.Enum           = RTS.RefType      LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` SM2         ON SM2.SourceTable     = 'SMRBIReqTransStaging'        AND SM2.SourceColumn   = 'Direction'        AND SM2.Enum           = RTS.Direction      LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` SM3         ON SM3.SourceTable     = 'SMRBIReqTransStaging'        AND SM3.SourceColumn   = 'ActionType'        AND SM3.Enum           = RTS.ActionType      LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` SM4         ON SM4.SourceTable     = 'SMRBIReqTransStaging'        AND SM4.SourceColumn   = 'ActionMarked'        AND SM4.Enum           = RTS.ActionMarked      LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` SM5         ON SM5.SourceTable     = 'SMRBIReqTransStaging'        AND SM5.SourceColumn   = 'FuturesMarked'        AND SM5.Enum           = RTS.FuturesMarked      LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` SM6         ON SM6.SourceTable     = 'SMRBIReqTransStaging'        AND SM6.SourceColumn   = 'FuturesCalculated'        AND SM6.Enum           = RTS.FuturesCalculated      LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIProdTableStaging` PTS         ON RTS.RefId   = PTS.ProdId        AND RTS.DataAreaId = PTS.COMPANY      LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIPurchPurchaseOrderHeaderStaging` POHS         ON RTS.RefId   = POHS.PurchaseOrderNumber        AND RTS.DataAreaId = POHS.DataAreaId      LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBISalesOrderHeaderStaging` SOHS         ON RTS.RefId   = SOHS.SalesOrderNumber        AND RTS.DataAreaId = SOHS.DataAreaId      LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIReqPOStaging` RPO         ON RTS.RefId      = RPO.RefId        AND RTS.DataAreaId = RPO.DataAreaId        AND RTS.PlanVersion = RPO.PlanVersion ) SELECT     RecId,     CompanyCode,     ReferenceType,     PlanVersion,     ProductCode,     InventDimCode,     RequirementDate,     RequirementTime,     RequirementDateTime,     ReferenceCode,     ProducedItemCode,     CustomerCode,     VendorCode,     ActionDate,     ActionDays,     ActionType,     ActionMarked,     FuturesDate,     FuturesDays,     FuturesCalculated,     FuturesMarked,     Direction,     CASE         WHEN Base.RefType = 14 THEN 0         ELSE Base._rn     END                                                AS RankNr,     Quantity,     CASE         WHEN Base.RefType IN (31, 32, 33, 34, 43) THEN 0         ELSE Base.Quantity     END                                                AS QuantityConfirmed FROM base AS Base
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
