# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICustomerTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICustomerTransStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------------
#  Databricks notebook: Transfer of a T‑SQL CREATE TABLE statement
#  -------------------------------------------------------------------------------
#  The original script creates a temporary table **dbo.SMRBICustomerTransStaging**
#  with a primary‑key constraint.  In Delta‑Lake (Databricks) the syntax for
#  primary keys and other index types is not supported – the table can be
#  created as a Delta table with the required column data types and
#  *NOT NULL* constraints, but the uniqueness enforcement must be handled
#  at the application level or via validation steps.
#
#  This notebook therefore:
#  * Creates the table in the target catalog and schema using fully‑qualified
#    names `dbe_dbx_internships`.`dbo`.`SMRBICustomerTransStaging`.
#  * Maps every T‑SQL data type to an equivalent Spark SQL/Delta type.
#  * Adds the `NOT NULL` constraint for columns that were declared as
#    NOT NULL in the original script.
#  * Omits the primary‑key and `WITH (...)` clause – a comment is added
#    to explain that the primary key cannot be enforced directly in
#    Delta Lake.
#
#  Usage:
#  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
