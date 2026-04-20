# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBOMCalcTransStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBOMCalcTransStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣ Create the staging table in Delta Lake
#
#    *  All object names are fully‑qualified:
#          dbe_dbx_internships.dbo.SMRBIBOMCalcTransStaging
#    *  The data types have been mapped as described in the
#          “Data Type Mapping” section above.
#    *  The original T‑SQL `PRIMARY KEY` definition is not supported
#          in Delta Lake; the constraint has been commented out
#          for reference, but it is not created in the catalog.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBOMCalcTransStaging` (
    DEFINITIONGROUP   STRING NOT NULL,
    EXECUTIONID       STRING NOT NULL,
    ISSELECTED        INT    NOT NULL,
    TRANSFERSTATUS    INT    NOT NULL,
    BOM               INT    NOT NULL,
    BOMCALCTRANSRECID BIGINT NOT NULL,
    CALCGROUPID       STRING NOT NULL,
    CALCTYPE          INT    NOT NULL,
    CONSISTOFPRICE    STRING NOT NULL,
    CONSUMPTIONCONSTANT DECIMAL(32,16) NOT NULL,
    CONSUMPTIONVARIABLE DECIMAL(32,16) NOT NULL,
    CONSISTTYPE       INT    NOT NULL,
    COSTCALCULATIONMETHOD INT NOT NULL,
    COSTGROUPID        STRING NOT NULL,
    COSTMARKUP         DECIMAL(32,16) NOT NULL,
    COSTMARKUPQTY       DECIMAL(32,16) NOT NULL,
    COSTPRICE           DECIMAL(32,16) NOT NULL,
    COSTPRICEMODELUSED  INT NOT NULL,
    COSTPRICEQTY         DECIMAL(32,16) NOT NULL,
    COSTPRICEUNIT       DECIMAL(32,12) NOT NULL,
    COMPANY             STRING NOT NULL,
    INVENTDIMID         STRING NOT NULL,
    LEVEL_              INT NOT NULL,
    LINENUM             DECIMAL(32,16) NOT NULL,
    NETWEIGHTQTY        DECIMAL(32,12) NOT NULL,
    NUMOFSERIES         DECIMAL(32,6)  NOT NULL,
    OPRID               STRING NOT NULL,
    OPRNUM              INT    NOT NULL,
    OPRNUMNEXT          INT    NOT NULL,
    OPRPRIORITY         INT    NOT NULL,
    PARENTBOMCALCTRANS  BIGINT NOT NULL,
    PRICECALCID         STRING NOT NULL,
    QTY                 DECIMAL(32,6)  NOT NULL,
    RESOURCE_           STRING NOT NULL,
    SALESMARKUP         DECIMAL(32,6)  NOT NULL,
    SALESMARKUPQTY      DECIMAL(32,6)  NOT NULL,
    SALESPRICE          DECIMAL(32,6)  NOT NULL,
    SALESPRICEQTY       DECIMAL(32,6)  NOT NULL,
    SALESPRICEUNIT      DECIMAL(32,12) NOT NULL,
    TRANSDATE           TIMESTAMP NOT NULL,
    UNITID              STRING NOT NULL,
    PARTITION           STRING NOT NULL,
    DATAAREAID           STRING NOT NULL,
    SYNCSTARTDATETIME   TIMESTAMP NOT NULL,
    RECID                BIGINT NOT NULL
    
    /* --------------------------------------------------------------------
       NOTE:  The original T‑SQL "PRIMARY KEY CLUSTERED" definition cannot
               be expressed directly in Delta Lake.  If you require
               uniqueness guarantees you can create a unique constraint
               via Delta's schema‑evolution API or enforce it in your
               application logic.
       -------------------------------------------------------------------- */
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣ (Optional) Persist the table in the catalog if it might be
#     accessed from other notebooks or from the Databricks UI
# ------------------------------------------------------------------
# `spark.catalog.createTable` can be used for this purpose, but the
# `CREATE TABLE USING DELTA` above already registers the table
# in the Hive metastore that Databricks uses.

# ------------------------------------------------------------------
# 3️⃣ Confirm the schema was created correctly
# ------------------------------------------------------------------
df_schema = spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIBOMCalcTransStaging`")
display(df_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
