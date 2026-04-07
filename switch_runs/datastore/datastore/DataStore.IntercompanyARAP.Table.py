# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.IntercompanyARAP.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.IntercompanyARAP.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------------
#  Create the IntercompanyARAP table in Databricks (Delta Lake)
# --------------------------------------------------------------------------------
# 1)  Each column definition is translated from T‑SQL to Spark SQL data types
#     using the mapping rules from the specification:
#
#      •  DATETIME / DATETIME2 → TIMESTAMP
#      •  NVARCHAR(MAX)         → STRING
#      •  INT                   → INT
#      •  VARCHAR(4)            → STRING   (no length restriction in Spark)
#      •  NUMERIC(38,8)          → DECIMAL(38,8)
#      •  NUMERIC(32,16)         → DECIMAL(32,16)
#
# 2)  The `ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]` clause is specific to SQL
#     Server and has no equivalent in Delta Lake / Spark SQL, so it is
#     omitted.  A comment explains this omission.
#
# 3)  The table is created with `CREATE OR REPLACE TABLE` so that the
#     notebook can be re‑run without error even if the table already
#     exists.
#
# 4)  Fully‑qualified names use the provided `dbe_dbx_internships` and `datastore`
#     placeholders.  Users should replace those placeholders with the
#     actual catalog and schema names before running the notebook.
#
# 5)  All column names are left unchanged from the source to preserve
#     compatibility with downstream code that expects those exact names.
#
# --------------------------------------------------------------------------------

catalog = "dbe_dbx_internships"
schema  = "datastore"
table   = "IntercompanyARAP"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`{table}` (
    InvoiceDate          TIMESTAMP,
    YearInvoice          INT,
    PostedDate           TIMESTAMP,
    DueDate              TIMESTAMP,
    Brn                  STRING NOT NULL,
    Departement          STRING NOT NULL,
    AR_AP_Type           STRING NOT NULL,
    Type                 STRING NOT NULL,
    SalesInvoiceCode     STRING NOT NULL,
    PurchaseInvoiceCode STRING NOT NULL,
    JobInvoice           STRING NOT NULL,
    Currency             STRING NOT NULL,
    InvoiceTotal         DECIMAL(38,8) NOT NULL,
    CustomerCode         STRING NOT NULL,
    SupplierCode         STRING NOT NULL,
    AccountName          STRING NOT NULL,
    AP_Settlement        STRING NOT NULL,
    AR_Settlement        STRING NOT NULL,
    CrGRP                STRING NOT NULL,
    DrGRP                STRING NOT NULL,
    DestDisch            STRING NOT NULL,
    ETA                  TIMESTAMP,
    ETD                  TIMESTAMP,
    House                STRING NOT NULL,
    JobNumber            STRING NOT NULL,
    Master               STRING NOT NULL,
    OrigCountry          STRING NOT NULL,
    OrigCountryName      STRING NOT NULL,
    OriginLoad           STRING NOT NULL,
    CompanyCode          STRING NOT NULL,
    ExchangeRate         DECIMAL(32,16) NOT NULL
)
""")

# COMMAND ----------

# Verify that the table was created by displaying its schema
df = spark.table(f"dbe_dbx_internships.datastore.{table}")
print(f"Table `dbe_dbx_internships.datastore.{table}` created with {len(df.columns)} columns.")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
