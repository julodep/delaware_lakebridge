# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProdRouteStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProdRouteStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the SMRBIProdRouteStaging staging table in Databricks.
#
# The original T‑SQL table is:
#   dbo.SMRBIProdRouteStaging
#   Columns:  - DEFINITIONGROUP (nvarchar(60))
#             - EXECUTIONID   (nvarchar(90))
#             - ISSELECTED    (int)
#             - TRANSFERSTATUS(int)
#             - CALCPROC      (numeric(32,6))
#             - CALCSETUP     (numeric(32,6))
#             - COMPANY       (nvarchar(4))
#             - FROMDATE      (datetime)
#             - OPRID         (nvarchar(10))
#             - OPRNUM        (int)
#             - OPRNUMNEXT    (int)
#             - PRODID        (nvarchar(20))
#             - ROUTEOPRREFRECID (bigint)
#             - TODATE        (datetime)
#             - PARTITION     (nvarchar(20))
#             - SYNCSTARTDATETIME (datetime)
#             - RECID         (bigint)
#
# SQL Server type mapping → Spark (Delta Lake) type mapping:
#   nvarchar → STRING
#   int      → INT
#   numeric  → DECIMAL(p,s)
#   datetime → TIMESTAMP
#
# Primary‑key constraints are not natively enforced in Delta Lake; we keep
# the logical columns together but add a comment that the key exists in the
# original design.
#
# --------------------------------------------------------------

# Drop the table if it already exists to avoid errors on repeated
# execution of the notebook cell.
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProdRouteStaging`")

# COMMAND ----------

# Create the table with the appropriate column definitions.
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProdRouteStaging` (
    DEFINITIONGROUP      STRING   NOT NULL,
    EXECUTIONID          STRING   NOT NULL,
    ISSELECTED           INT      NOT NULL,
    TRANSFERSTATUS       INT      NOT NULL,
    CALCPROC             DECIMAL(32, 6) NOT NULL,
    CALCSETUP            DECIMAL(32, 6) NOT NULL,
    COMPANY              STRING   NOT NULL,
    FROMDATE              TIMESTAMP NOT NULL,
    OPRID                STRING   NOT NULL,
    OPRNUM               INT      NOT NULL,
    OPRNUMNEXT           INT      NOT NULL,
    PRODID               STRING   NOT NULL,
    ROUTEOPRREFRECID     BIGINT   NOT NULL,
    TODATE                TIMESTAMP NOT NULL,
    PARTITION            STRING   NOT NULL,
    SYNCSTARTDATETIME    TIMESTAMP NOT NULL,
    RECID                BIGINT   NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
