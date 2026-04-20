# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBOMBillOfMaterialsLineStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBOMBillOfMaterialsLineStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#   Create the Delta table that maps to the T‑SQL table  
#   dbo.SMRBIBOMBillOfMaterialsLineStaging
# ------------------------------------------------------------------
# Each data type is translated to the closest Spark SQL equivalent:
#   * NVARCHAR       -> STRING
#   * NUMERIC(p,s)   -> DECIMAL(p,s)
#   * DATETIME      -> TIMESTAMP
#   * BIGINT         -> LONG
#   * INT            -> INT
# ------------------------------------------------------------------

# Create the table with the correct schema and fully‑qualified name.
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBOMBillOfMaterialsLineStaging` (
    DEFINITIONGROUP            STRING   NOT NULL,
    EXECUTIONID                STRING   NOT NULL,
    ISSELECTED                 INT      NOT NULL,
    TRANSFERSTATUS             INT      NOT NULL,
    CONSUMPTIONTYPE            INT      NOT NULL,
    BOMID                      STRING   NOT NULL,
    QUANTITY                   DECIMAL(32,6) NOT NULL,
    QUANTITYDENOMINATOR        DECIMAL(32,6) NOT NULL,
    LINETYPE                   INT      NOT NULL,
    WILLCOSTCALCULATIONINCLUDELINE INT    NOT NULL,
    CONSUMPTIONCALCULATIONCONSTANT DECIMAL(32,6) NOT NULL,
    PHYSICALPRODUCTDENSITY     DECIMAL(32,6) NOT NULL,
    PHYSICALPRODUCTDEPTH       DECIMAL(32,6) NOT NULL,
    ISCONSUMEDATOPERATIONCOMPLETE INT    NOT NULL,
    CONSUMPTIONCALCULATIONMETHOD INT    NOT NULL,
    VALIDFROMDATE              TIMESTAMP NOT NULL,
    PHYSICALPRODUCTHEIGHT      DECIMAL(32,6) NOT NULL,
    SUBBOMID                   STRING   NOT NULL,
    ITEMNUMBER                 STRING   NOT NULL,
    SUBROUTEID                 STRING   NOT NULL,
    LINENUMBER                 DECIMAL(32,16) NOT NULL,
    ROUTEOPERATIONNUMBER       INT      NOT NULL,
    CATCHWEIGHTQUANTITY        DECIMAL(32,6) NOT NULL,
    WILLMANUFACTUREDITEMINHERITBATCHATTRIBUTES INT NOT NULL,
    WILLMANUFACTUREDITEMINHERITSHELFLIFEDATES INT NOT NULL,
    POSITIONNUMBER              STRING   NOT NULL,
    FLUSHINGPRINCIPLE           INT      NOT NULL,
    ROUNDINGUPMETHOD            INT      NOT NULL,
    QUANTITYROUNDINGUPMULTIPLES DECIMAL(32,6) NOT NULL,
    CONSTANTSCRAPQUANTITY       DECIMAL(32,6) NOT NULL,
    VARIABLESCRAPPERCENTAGE     DECIMAL(32,6) NOT NULL,
    VALIDTODATE                 TIMESTAMP NOT NULL,
    PRODUCTUNITSYMBOL           STRING   NOT NULL,
    VENDORACCOUNTNUMBER         STRING   NOT NULL,
    PHYSICALPRODUCTWIDTH        DECIMAL(32,6) NOT NULL,
    ISRESOURCECONSUMPTIONUSED   INT      NOT NULL,
    PRODUCTCONFIGURATIONID      STRING   NOT NULL,
    PRODUCTCOLORID              STRING   NOT NULL,
    PRODUCTSIZEID               STRING   NOT NULL,
    PRODUCTSTYLEID              STRING   NOT NULL,
    CONSUMPTIONSITEID           STRING   NOT NULL,
    CONSUMPTIONWAREHOUSEID      STRING   NOT NULL,
    CONFIGGROUPID               STRING   NOT NULL,
    PARTITION                   STRING   NOT NULL,
    DATAAREAID                  STRING   NOT NULL,
    SYNCSTARTDATETIME           TIMESTAMP NOT NULL,
    RECID                       LONG     NOT NULL
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
#   Note: The original T‑SQL definition contained a PRIMARY KEY and
#   various table options (ON [PRIMARY], STATISTICS_NORECOMPUTE, …).
#   Delta Lake does not enforce primary keys or these options.  We
#   add an informational table comment so future readers understand
#   the origin of the table.
# ------------------------------------------------------------------
spark.sql(f"""
COMMENT ON TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBOMBillOfMaterialsLineStaging`
IS 'Table originally created in T‑SQL with a composite primary key.
Primary key constraints are not enforced in Delta Lake.  The schema
has been mapped to Spark SQL types as follows:
- NVARCHAR -> STRING
- NUMERIC(p,s) -> DECIMAL(p,s)
- DATETIME -> TIMESTAMP
- BIGINT -> LONG
- INT -> INT';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
