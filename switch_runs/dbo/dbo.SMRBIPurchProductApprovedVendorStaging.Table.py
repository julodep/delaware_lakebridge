# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIPurchProductApprovedVendorStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIPurchProductApprovedVendorStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Databricks notebook – Convert T‑SQL CREATE TABLE to Delta Lake table
#  Catalog: <catalog>
#  Schema:   <schema>
#  ---------------------------------------------------------------
#  The original T‑SQL statement creates a primary‑key clustered table in
#  a traditional relational database.
#
#  Delta Lake (Databricks) does not support most of the DDL options that
#  were used here – e.g. PRIMARY KEY constraints, statistics settings,
#  or “CLUSTERED ( … )” syntax.  The conversion therefore boils down
#  to:
#     •  Create the table under the supplied catalog and schema
#     •  Map each column to an equivalent Spark SQL data type
#     •  Omit the PRIMARY KEY clause; in practice you can keep a unique
#        key column or define a Delta Lake “UNIQUE” key later, if needed.
#
#  Column mapping
#  ---------------------------------------------
#        T‑SQL type   Spark type
#        nvarchar    STRING
#        int         INT
#        datetime    TIMESTAMP
#        bigint      BIGINT
#  ---------------------------------------------
#  The fully‑qualified name is `dbe_dbx_internships.dbo.SMRBIPurchProductApprovedVendorStaging`.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIPurchProductApprovedVendorStaging`
(
  DEFINITIONGROUP STRING   NOT NULL,
  EXECUTIONID     STRING   NOT NULL,
  ISSELECTED      INT      NOT NULL,
  TRANSFERSTATUS  INT      NOT NULL,
  ITEMNUMBER      STRING   NOT NULL,
  APPROVEDVENDORACCOUNTNUMBER STRING NOT NULL,
  EFFECTIVEDATE   TIMESTAMP NOT NULL,
  COMPANY         STRING   NOT NULL,
  PARTITION       STRING   NOT NULL,
  DATAAREAID      STRING   NOT NULL,
  SYNCSTARTDATETIME TIMESTAMP NOT NULL,
  RECID           BIGINT   NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
