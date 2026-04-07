# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Facility.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Facility.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Create the DataStore.Facility table in the target Delta Lake
# ------------------------------------------------------------
# NOTE:  Spark/Deltalake does not allow a plain column list in
# CREATE TABLE without a data source.  We create the empty table
# via a DataFrame with the desired schema and write it as Delta.
# ------------------------------------------------------------

from pyspark.sql.types import StructType, StructField, LongType, StringType

# COMMAND ----------

# Define the schema for the Facility table
facility_schema = StructType(
    [
        StructField("FacilityId",     LongType(),     nullable=True),
        StructField("FacilityCode",   StringType(),   nullable=True),
        StructField("FacilityName",   StringType(),   nullable=True),
        StructField("FacilityCodeName", StringType(), nullable=True),
        StructField("DimensionName",  StringType(),   nullable=True),
    ]
)

# COMMAND ----------

# Create an empty DataFrame with the defined schema
empty_df = spark.createDataFrame([], facility_schema)

# COMMAND ----------

# Write the empty DataFrame as a Delta table, creating the table if it does not yet exist
empty_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"`dbe_dbx_internships`.`datastore`.`Facility`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
