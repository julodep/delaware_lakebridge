# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.sp_ssis_addlogentry.StoredProcedure
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.sp_ssis_addlogentry.StoredProcedure.sql`

# COMMAND ----------

# -----------------------------------------------------------
#  Databricks Python notebook – emulation of
#  CREATE PROCEDURE dbo.sp_ssis_addlogentry
# -----------------------------------------------------------

# -----------------------------------------------------------
#  1. Define the target catalog + schema
# -----------------------------------------------------------
catalog = "dbe_dbx_internships"
schema  = "dbo"

# COMMAND ----------

# -----------------------------------------------------------
#  2. Ensure the target table exists.
#    The table definition mirrors the T‑SQL data types:
#        sysname           -> STRING
#        nvarchar(n)       -> STRING
#        uniqueidentifier  -> STRING  (store UUID as string)
#        datetime          -> TIMESTAMP
#        int               -> INT
#        image             -> BINARY
# -----------------------------------------------------------
table_name = f"`dbe_dbx_internships`.`dbo`.`sysssislog`"

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {table_name} (
      event          STRING,
      computer       STRING,
      operator       STRING,
      source         STRING,
      sourceid       STRING,
      executionid    STRING,
      starttime      TIMESTAMP,
      endtime        TIMESTAMP,
      datacode       INT,
      databytes      BINARY,
      message        STRING
  )
""")

# COMMAND ----------

# -----------------------------------------------------------
#  3. Python helper – convert python primitives to spark types.
# -----------------------------------------------------------
from pyspark.sql.types import *
from pyspark.sql import Row
import sys
import uuid
from datetime import datetime

# COMMAND ----------

# -----------------------------------------------------------
#  4. Concrete implementation of the stored procedure.
# -----------------------------------------------------------
def sp_ssis_addlogentry(
        event:        str,
        computer:     str,
        operator:     str,
        source:       str,
        sourceid:     str,
        executionid: str,
        starttime:    datetime,
        endtime:      datetime,
        datacode:     int,
        databytes:   bytes,
        message:      str
    ) -> int:
    """
    Emulates the T‑SQL procedure dbo.sp_ssis_addlogentry.

    Parameters
    ----------
    event, computer, operator, source, message : str
    sourceid, executionid : str (UUID)
    starttime, endtime : datetime
    datacode : int
    databytes : bytes

    Returns
    -------
    int
        0 on success, mimicking the T‑SQL RETURN value.
    """
    try:
        uuid.UUID(sourceid)
        uuid.UUID(executionid)
    except ValueError as e:
        raise ValueError(f"Invalid UUID: {e}") from e

    row = Row(
        event        = event,
        computer     = computer,
        operator     = operator,
        source       = source,
        sourceid     = sourceid,
        executionid  = executionid,
        starttime    = starttime,
        endtime      = endtime,
        datacode     = datacode,
        databytes    = databytes,
        message      = message
    )
    df = spark.createDataFrame([row])

    try:
        df.write.mode("append").insertInto(table_name, overwrite=False)
    except Exception as e:
        print(f"[sp_ssis_addlogentry] Insert failed: {e}", file=sys.stderr)
        raise

    return 0

# COMMAND ----------

# -----------------------------------------------------------
#  8. Example usage (can be removed or commented out)
# -----------------------------------------------------------
if __name__ == "__main__":
    ev           = "Migrate"
    comp         = "my-computer"
    op           = "admin"
    src          = "ETL-Source"
    sid          = str(uuid.uuid4())
    eid          = str(uuid.uuid4())
    st           = datetime.now()
    et           = datetime.now()
    dcode        = 200
    dbytes       = b'\x01\x02\x03'
    msg          = "Migration completed successfully."

    result = sp_ssis_addlogentry(ev, comp, op, src, sid, eid,
                                 st, et, dcode, dbytes, msg)
    print(f"[sp_ssis_addlogentry] procedure returned {result}")

    spark.sql(f"SELECT * FROM {table_name} ORDER BY starttime DESC LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
