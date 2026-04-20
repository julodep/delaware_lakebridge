# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICaseCategoryHierarchyDetailStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICaseCategoryHierarchyDetailStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------
# Create the staging table `SMRBICaseCategoryHierarchyDetailStaging`
# in the specified Unity Catalog namespace (`dbe_dbx_internships`.dbo`).
# ---------------------------------------------------------
# NOTE: 
#   - Spark/Delta Lake does not support native PRIMARY KEY constraints,
#     so the T‑SQL PK definition is omitted.
#   - If you require uniqueness enforcement, enforce it at the application
#     or query level (e.g., via DISTINCT, window functions, or a
#     separate unique‑key check in the code).
#   - All columns are mapped to Spark SQL data types:
#        NVARCHAR → STRING
#        INT      → INT
#        BIGINT   → LONG
#        DATETIME → TIMESTAMP
# ---------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBICaseCategoryHierarchyDetailStaging
(
  DEFINITIONGROUP STRING NOT NULL,
  EXECUTIONID STRING NOT NULL,
  ISSELECTED INT NOT NULL,
  TRANSFERSTATUS INT NOT NULL,
  CASECATEGORY STRING NOT NULL,
  CATEGORYTYPE INT NOT NULL,
  CREATEACTIVITYFORCASE INT NOT NULL,
  CREATEACTIVITYFORFOLLOWUP INT NOT NULL,
  COMPANY STRING NOT NULL,
  DEFAULTACTIVITYCATEGORY INT NOT NULL,
  DEFAULTACTIVITYPHASE STRING NOT NULL,
  DEFAULTACTIVITYPURPOSE STRING NOT NULL,
  DEFAULTACTIVITYTYPE STRING NOT NULL,
  DEFAULTFOLLOWUPCATEGORY INT NOT NULL,
  DEFAULTFOLLOWUPPHASE STRING NOT NULL,
  DEFAULTFOLLOWUPPURPOSE STRING NOT NULL,
  DEFAULTFOLLOWUPTYPE STRING NOT NULL,
  DEFAULTOWNERWORKER LONG NOT NULL,
  DEPARTMENT LONG NOT NULL,
  DESCRIPTION STRING NOT NULL,
  EMAILID STRING NOT NULL,
  PARENTRECID LONG NOT NULL,
  PROCESS STRING NOT NULL,
  CASECATEGORYHIERARCHYRECID LONG NOT NULL,
  CASECATEGORYHIERARCHYRECVERSION INT NOT NULL,
  SERVICELEVELAGREEMENTID STRING NOT NULL,
  PARTITION STRING NOT NULL,
  DATAAREAID STRING NOT NULL,
  SYNCSTARTDATETIME TIMESTAMP NOT NULL,
  RECID LONG NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
