# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_InterCompanyARAP.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_InterCompanyARAP.View.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Databricks notebook for creating the view `DataStore.V_InterCompanyARAP`
# --------------------------------------------------------------------------
#
# NOTE:
# - All object names are fully-qualified using the catalog and schema placeholders.
#   Replace dbe_dbx_internships and datastore with the actual catalog / schema you target.
# - T‑SQL functions were translated to their Spark SQL equivalents:
#     * CONVERT(date, expr)      -> cast(expr AS date)
#     * ISNULL(expr, repl)       -> coalesce(expr, repl)
#     * YEAR(expr)               -> year(expr)
#     * CAST(... AS DECIMAL(p,s))-> cast(... AS DECIMAL(p,s))
#   The rest of the logic remains unchanged.
# - Column names that contain special characters or spaces are wrapped in
#   back‑ticks (`) so that Spark can reference them.
# - The view is created (or replaced) persistently in Unity Catalog so it
#   is available to all notebooks/users in the workspace.
#
# --------------------------------------------------------------------------

# Define your catalog and schema
catalog = "dbe_dbx_internships"
schema  = "datastore"

# COMMAND ----------

# Full view name
view_name = f"dbe_dbx_internships.datastore.V_InterCompanyARAP"

# COMMAND ----------

# =============================================================================
# Build the view definition
# =============================================================================
view_sql = f"""
CREATE OR REPLACE VIEW `{view_name}` AS
SELECT
    -- 1. Invoice date (converted to datetime)
    cast(coalesce(`Invoice`, '1900-01-01') AS date)                                       AS InvoiceDate,
    year(cast(coalesce(`Invoice`, '1900-01-01') AS date))                                 AS YearInvoice,

    cast(coalesce(`Posted`, '1900-01-01') AS date)                                         AS PostedDate,

    cast(coalesce(`Due`, '1900-01-01') AS date)                                           AS DueDate,

    coalesce(`Brn.`, '_N/A')                                                             AS Brn,

    coalesce(`Dept`, '_N/A')                                                             AS Departement,

    coalesce(`Lgr.`, '_N/A')                                                             AS AR_AP_Type,

    coalesce(`Type`, '_N/A')                                                             AS Type,

    CASE WHEN `Lgr.` = 'AR'
         THEN coalesce(`Transaction #`, '_N/A')
         ELSE '_N/A' END                                                                AS SalesInvoiceCode,

    CASE WHEN `Lgr.` = 'AP'
         THEN coalesce(`Transaction #`, '_N/A')
         ELSE '_N/A' END                                                                AS PurchaseInvoiceCode,

    coalesce(`Job Invoice / Posting Ref`, '_N/A')                                         AS JobInvoice,

    coalesce(`Cur`, '_N/A')                                                             AS Currency,

    coalesce(cast(`Invoice Total` AS decimal(38,8)), 0)                                   AS InvoiceTotal,

    CASE WHEN `Lgr.` = 'AR'
         THEN coalesce(`Account`, '_N/A')
         ELSE '_N/A' END                                                                AS CustomerCode,

    CASE WHEN `Lgr.` = 'AP'
         THEN coalesce(`Account`, '_N/A')
         ELSE '_N/A' END                                                                AS SupplierCode,

    coalesce(`Account Name`, '_N/A')                                                    AS AccountName,

    coalesce(`AP Settlement`, '_N/A')                                                   AS AP_Settlement,

    coalesce(`AR Settlement`, '_N/A')                                                   AS AR_Settlement,

    coalesce(`CR GRP`, '_N/A')                                                          AS CrGRP,

    coalesce(`DR GRP`, '_N/A')                                                          AS DrGRP,

    coalesce(`Dest. / Disch.`, '_N/A')                                                  AS DestDisch,

    cast(coalesce(`ETA`, '1900-01-01') AS timestamp)                                   AS ETA,

    cast(coalesce(`ETD`, '1900-01-01') AS timestamp)                                   AS ETD,

    coalesce(`House`, '_N/A')                                                            AS House,

    coalesce(`Job Number`, '_N/A')                                                      AS JobNumber,

    coalesce(`Master`, '_N/A')                                                          AS `Master`,

    coalesce(`Org. Country `, '_N/A')                                                   AS OrigCountry,

    coalesce(`Org. Country Name `, '_N/A')                                               AS OrigCountryName,

    coalesce(`Origin / Load`, '_N/A')                                                   AS OriginLoad,

    CompanyCode,

    coalesce(cast(`Exchange` AS decimal(32,16)), 0)                                       AS ExchangeRate

FROM (
    /* ------------------------------------------------------------------------
       Union of the different country/stage tables
       ------------------------------------------------------------------------ */
    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'EU10'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_EU10`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'BE20'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_BE20`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'BX20'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_BX20`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'EM14'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_EM14`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'LU20'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_LU20`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'NL20'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_NL20`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'SE20'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_SE20`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'FR50'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_FR50`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'DE30'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_DE30`
    WHERE `Invoice` IS NOT NULL

    UNION ALL

    SELECT  `Invoice`,
            `Posted`,
            `Due`,
            `Brn.`,
            `Dept`,
            `Lgr.`,
            `Type`,
            `Transaction #`,
            `Job Invoice / Posting Ref`,
            `Cur`,
            `Invoice Total`,
            `Exchange`,
            `Local Total`,
            `Outstanding OS Currency`,
            `Outstanding Local Equiv`,
            `Account`,
            `Account Name`,
            `AP Settlement`,
            `AR Settlement`,
            `CR GRP`,
            `DR GRP`,
            `Dest. / Disch.`,
            `ETA`,
            `ETD`,
            `House`,
            `Job Number`,
            `Master`,
            `Org. Country `,
            `Org. Country Name `,
            `Origin / Load`,
            'SW31'      AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`ARAP_SW31`
    WHERE `Invoice` IS NOT NULL
) a
"""

# COMMAND ----------

# --------------------------------------------------------------------------
# Execute the view creation
# --------------------------------------------------------------------------
spark.sql(view_sql)

# COMMAND ----------

print(f"View `{view_name}` has been created (or replaced) successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 12767)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `catalog.schema.V_InterCompanyARAP` AS SELECT     -- 1. Invoice date (converted to datetime)     cast(coalesce(`Invoice`, '1900-01-01') AS date)                                       AS InvoiceDate,     year(cast(coalesce(`Invoice`, '1900-01-01') AS date))                                 AS YearInvoice,      cast(coalesce(`Posted`, '1900-01-01') AS date)                                         AS PostedDate,      cast(coalesce(`Due`, '1900-01-01') AS date)                                           AS DueDate,      coalesce(`Brn.`, '_N/A')                                                             AS Brn,      coalesce(`Dept`, '_N/A')                                                             AS Departement,      coalesce(`Lgr.`, '_N/A')                                                             AS AR_AP_Type,      coalesce(`Type`, '_N/A')                                                             AS Type,      CASE WHEN `Lgr.` = 'AR'          THEN coalesce(`Transaction #`, '_N/A')          ELSE '_N/A' END                                                                AS SalesInvoiceCode,      CASE WHEN `Lgr.` = 'AP'          THEN coalesce(`Transaction #`, '_N/A')          ELSE '_N/A' END                                                                AS PurchaseInvoiceCode,      coalesce(`Job Invoice / Posting Ref`, '_N/A')                                         AS JobInvoice,      coalesce(`Cur`, '_N/A')                                                             AS Currency,      coalesce(cast(`Invoice Total` AS decimal(38,8)), 0)                                   AS InvoiceTotal,      CASE WHEN `Lgr.` = 'AR'          THEN coalesce(`Account`, '_N/A')          ELSE '_N/A' END                                                                AS CustomerCode,      CASE WHEN `Lgr.` = 'AP'          THEN coalesce(`Account`, '_N/A')          ELSE '_N/A' END                                                                AS SupplierCode,      coalesce(`Account Name`, '_N/A')                                                    AS AccountName,      coalesce(`AP Settlement`, '_N/A')                                                   AS AP_Settlement,      coalesce(`AR Settlement`, '_N/A')                                                   AS AR_Settlement,      coalesce(`CR GRP`, '_N/A')                                                          AS CrGRP,      coalesce(`DR GRP`, '_N/A')                                                          AS DrGRP,      coalesce(`Dest. / Disch.`, '_N/A')                                                  AS DestDisch,      cast(coalesce(`ETA`, '1900-01-01') AS timestamp)                                   AS ETA,      cast(coalesce(`ETD`, '1900-01-01') AS timestamp)                                   AS ETD,      coalesce(`House`, '_N/A')                                                            AS House,      coalesce(`Job Number`, '_N/A')                                                      AS JobNumber,      coalesce(`Master`, '_N/A')                                                          AS `Master`,      coalesce(`Org. Country `, '_N/A')                                                   AS OrigCountry,      coalesce(`Org. Country Name `, '_N/A')                                               AS OrigCountryName,      coalesce(`Origin / Load`, '_N/A')                                                   AS OriginLoad,      CompanyCode,      coalesce(cast(`Exchange` AS decimal(32,16)), 0)                                       AS ExchangeRate  FROM (     /* ------------------------------------------------------------------------        Union of the different country/stage tables        ------------------------------------------------------------------------ */     SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'EU10'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_EU10`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'BE20'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_BE20`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'BX20'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_BX20`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'EM14'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_EM14`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'LU20'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_LU20`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'NL20'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_NL20`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'SE20'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_SE20`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'FR50'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_FR50`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'DE30'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_DE30`     WHERE `Invoice` IS NOT NULL      UNION ALL      SELECT  `Invoice`,             `Posted`,             `Due`,             `Brn.`,             `Dept`,             `Lgr.`,             `Type`,             `Transaction #`,             `Job Invoice / Posting Ref`,             `Cur`,             `Invoice Total`,             `Exchange`,             `Local Total`,             `Outstanding OS Currency`,             `Outstanding Local Equiv`,             `Account`,             `Account Name`,             `AP Settlement`,             `AR Settlement`,             `CR GRP`,             `DR GRP`,             `Dest. / Disch.`,             `ETA`,             `ETD`,             `House`,             `Job Number`,             `Master`,             `Org. Country `,             `Org. Country Name `,             `Origin / Load`,             'SW31'      AS CompanyCode     FROM `catalog`.`schema`.`ARAP_SW31`     WHERE `Invoice` IS NOT NULL ) a
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
