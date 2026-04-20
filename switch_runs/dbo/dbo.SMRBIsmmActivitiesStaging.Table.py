# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIsmmActivitiesStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIsmmActivitiesStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the SMRBIsmmActivitiesStaging table in the target catalog/schema.
# ------------------------------------------------------------------

# Define the fully‑qualified table name using the supplied placeholders
table_name = f"`dbe_dbx_internships`.`dbo`.`SMRBIsmmActivitiesStaging`"

# COMMAND ----------

# Drop the table if it already exists to avoid conflicts when the notebook is run multiple times
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# COMMAND ----------

# ------------------------------------------------------------------
# Create the table with the appropriate column definitions.
# ------------------------------------------------------------------
sql_create = f"""
CREATE TABLE {table_name} (
    `DEFINITIONGROUP` STRING NOT NULL,
    `EXECUTIONID` STRING NOT NULL,
    `ISSELECTED` INT NOT NULL,
    `TRANSFERSTATUS` INT NOT NULL,
    `ACTUALWORK` DECIMAL(32,6) NOT NULL,
    `ACTIVITYNUMBER` STRING NOT NULL,
    `ACTIVITYTASKTIMETYPE` INT NOT NULL,
    `ACTIVITYTIMETYPE` INT NOT NULL,
    `ACTUALENDDATETIME` TIMESTAMP NOT NULL,
    `ALLDAY` INT NOT NULL,
    `BILLINGINFORMATION` STRING NOT NULL,
    `CATEGORY` INT NOT NULL,
    `CLOSED` INT NOT NULL,
    `COMPANY` STRING NOT NULL,
    `DONEBYWORKER` BIGINT NOT NULL,
    `ENDDATETIME` TIMESTAMP NOT NULL,
    `EXTERNALMEMO` STRING,
    `ISMASTERAPPOINTMENT` INT NOT NULL,
    `ISTEMPLATE` INT NOT NULL,
    `KEEPSYNCHRONIZED` INT NOT NULL,
    `LASTEDITAXDATETIME` TIMESTAMP NOT NULL,
    `LOCATION` STRING NOT NULL,
    `MILEAGE` STRING NOT NULL,
    `MODIFIED` INT NOT NULL,
    `ORIGINALAPPOINTMENTSTARTDATETIME` TIMESTAMP NOT NULL,
    `OUTLOOKCATEGORIES` STRING NOT NULL,
    `OUTLOOKENTRYID` STRING NOT NULL,
    `OUTLOOKGLOBALOBJECTID` STRING NOT NULL,
    `OUTLOOKRESOURCES` STRING NOT NULL,
    `PERCENTAGECOMPLETED` DECIMAL(32,6) NOT NULL,
    `PHASEID` STRING NOT NULL,
    `PLANID` STRING NOT NULL,
    `PURPOSE` STRING NOT NULL,
    `RECURRENCESTATE` INT NOT NULL,
    `REMINDERACTIVE` INT NOT NULL,
    `REMINDERDATETIME` TIMESTAMP NOT NULL,
    `REMINDERMINUTES` INT NOT NULL,
    `RESPONSEREQUESTED` INT NOT NULL,
    `RESPONSIBLEWORKER` BIGINT NOT NULL,
    `SENSITIVITY` INT NOT NULL,
    `SOURCE` STRING NOT NULL,
    `STARTDATETIME` TIMESTAMP NOT NULL,
    `STATUS` INT NOT NULL,
    `TASKPRIORITY` INT NOT NULL,
    `TASKROLE` STRING NOT NULL,
    `TEAMTASK` INT NOT NULL,
    `TOTALWORK` DECIMAL(32,6) NOT NULL,
    `TYPEID` STRING NOT NULL,
    `USERMEMO` ARRAY<STRING>,
    `DONEBYWORKER_PERSONNELNUMBER` STRING NOT NULL,
    `RESPONSIBLEWORKER_PERSONNELNUMBER` STRING NOT NULL,
    `SMMACTIVITIESRECID` BIGINT NOT NULL,
    `PARTITION` STRING NOT NULL,
    `DATAAREAID` STRING NOT NULL,
    `SYNCSTARTDATETIME` TIMESTAMP NOT NULL
) USING DELTA
"""

# COMMAND ----------

spark.sql(sql_create)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
