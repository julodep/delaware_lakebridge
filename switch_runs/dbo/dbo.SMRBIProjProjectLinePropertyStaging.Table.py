# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjProjectLinePropertyStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjProjectLinePropertyStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the staging table SMRBIProjProjectLinePropertyStaging
# in the specified Unity Catalog catalog and schema.
# -------------------------------------------------------------

# NOTE: All identifiers are fully‑qualified using the dbe_dbx_internships and dbo
# variables that should be defined in the notebook or passed as widgets.
# This script assumes those variables are available in the notebook
# environment (e.g. catalog = "my_catalog", schema = "my_schema").

spark.sql(f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.SMRBIProjProjectLinePropertyStaging (
        DEFINITIONGROUP   STRING   NOT NULL,
        EXECUTIONID       STRING   NOT NULL,
        ISSELECTED        INT      NOT NULL,
        TRANSFERSTATUS    INT      NOT NULL,
        COSTPERCENTAGE    DECIMAL(32, 6) NOT NULL,
        COMPANY           STRING   NOT NULL,
        LINEPROPERTYID    STRING   NOT NULL,
        NAME             STRING   NOT NULL,
        SALESPERCENTAGE  DECIMAL(32, 6) NOT NULL,
        PARTITION         STRING   NOT NULL,
        DATAAREAID        STRING   NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL
    ) 
    USING DELTA
    -- The primary‑key information from T‑SQL is not enforced by Delta;
    -- it is retained as a comment for reference.
    COMMENT 'PRIMARY KEY (EXECUTIONID, LINEPROPERTYID, DATAAREAID, PARTITION)'
""")

# COMMAND ----------

# If you want to explicitly create a staged Delta table you can also
# register the schema in a dictionary and use spark.createDataFrame.
# Below is an illustrative example (commented out):

# #############################################################################
# # Example of creating an empty DataFrame with the same schema and saving it
# #############################################################################
# from pyspark.sql import Row
# schema = ["DEFINITIONGROUP","EXECUTIONID","ISSELECTED","TRANSFERSTATUS",
#           "COSTPERCENTAGE","COMPANY","LINEPROPERTYID","NAME",
#           "SALESPERCENTAGE","PARTITION","DATAAREAID","SYNCSTARTDATETIME"]
# empty_df = spark.createDataFrame([], schema=StructType([
#     StructField("DEFINITIONGROUP", StringType(), False),
#     StructField("EXECUTIONID", StringType(), False),
#     StructField("ISSELECTED", IntegerType(), False),
#     StructField("TRANSFERSTATUS", IntegerType(), False),
#     StructField("COSTPERCENTAGE", DecimalType(32, 6), False),
#     StructField("COMPANY", StringType(), False),
#     StructField("LINEPROPERTYID", StringType(), False),
#     StructField("NAME", StringType(), False),
#     StructField("SALESPERCENTAGE", DecimalType(32, 6), False),
#     StructField("PARTITION", StringType(), False),
#     StructField("DATAAREAID", StringType(), False),
#     StructField("SYNCSTARTDATETIME", TimestampType(), False)
# ]))
# empty_df.write.format("delta").mode("overwrite").saveAsTable(f"dbe_dbx_internships.dbo.SMRBIProjProjectLinePropertyStaging")

print("Table SMRBIProjProjectLinePropertyStaging has been created in ", f"dbe_dbx_internships.dbo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
