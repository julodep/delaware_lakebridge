# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ExchangeRate.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ExchangeRate.View.sql`

# COMMAND ----------

# Set the catalog name
catalog = "mycatalog"

# COMMAND ----------

# Create or replace the persistent view
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`DataStore`.`V_ExchangeRate` AS

-- 1️⃣ From normal exchange rates (forward direction)
SELECT DISTINCT
    ERES.RateTypeName          AS ExchangeRateTypeCode,
    ERES.RateTypeDescription   AS ExchangeRateTypeName,
    'Dynamics365'             AS DataSource,
    ERES.FromCurrency         AS FromCurrencyCode,
    ERES.ToCurrency           AS ToCurrencyCode,
    ERES.StartDate            AS ValidFrom,
    ERES.EndDate              AS ValidTo,
    ERES.EXCHANGERATE / 100   AS ExchangeRate
FROM `dbe_dbx_internships`.`DataStore`.`SMRBIExchangeRateStaging` ERES
CROSS JOIN  (SELECT groupCurrencyCode FROM `dbe_dbx_internships`.`ETL`.`GroupCurrency` LIMIT 1) gc

UNION ALL

-- 2️⃣ From normal exchange rates (reverse direction)
SELECT DISTINCT
    ERES.RateTypeName          AS ExchangeRateTypeCode,
    ERES.RateTypeDescription   AS ExchangeRateTypeName,
    'Dynamics365'             AS DataSource,
    ERES.ToCurrency           AS FromCurrencyCode,
    ERES.FromCurrency         AS ToCurrencyCode,
    ERES.StartDate            AS ValidFrom,
    ERES.EndDate              AS ValidTo,
    1 / (ERES.EXCHANGERATE / 100) AS ExchangeRate
FROM `dbe_dbx_internships`.`DataStore`.`SMRBIExchangeRateStaging` ERES
CROSS JOIN  (SELECT groupCurrencyCode FROM `dbe_dbx_internships`.`ETL`.`GroupCurrency` LIMIT 1) gc

UNION ALL

-- 3️⃣ Synthetic rows that assign the rate 1 when the source currency matches itself
SELECT DISTINCT
    ERES.RateTypeName          AS ExchangeRateTypeCode,
    ERES.RateTypeDescription   AS ExchangeRateTypeName,
    'Dynamics365'             AS DataSource,
    ERES.FromCurrency         AS FromCurrencyCode,
    ERES.FromCurrency         AS ToCurrencyCode,
    cast('1900-01-01' AS timestamp) AS ValidFrom,
    cast('9999-12-31' AS timestamp) AS ValidTo,
    1 AS ExchangeRate
FROM `dbe_dbx_internships`.`DataStore`.`SMRBIExchangeRateStaging` ERES
CROSS JOIN  (SELECT groupCurrencyCode FROM `dbe_dbx_internships`.`ETL`.`GroupCurrency` LIMIT 1) gc

UNION ALL

-- 4️⃣ Synthetic rows that assign the rate 1 when the target currency matches itself
SELECT DISTINCT
    ERES.RateTypeName          AS ExchangeRateTypeCode,
    ERES.RateTypeDescription   AS ExchangeRateTypeName,
    'Dynamics365'             AS DataSource,
    ERES.ToCurrency           AS FromCurrencyCode,
    ERES.ToCurrency           AS ToCurrencyCode,
    cast('1900-01-01' AS timestamp) AS ValidFrom,
    cast('9999-12-31' AS timestamp) AS ValidTo,
    1 AS ExchangeRate
FROM `dbe_dbx_internships`.`DataStore`.`SMRBIExchangeRateStaging` ERES
CROSS JOIN  (SELECT groupCurrencyCode FROM `dbe_dbx_internships`.`ETL`.`GroupCurrency` LIMIT 1) gc;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2590)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `mycatalog`.`DataStore`.`V_ExchangeRate` AS  -- 1️⃣ From normal exchange rates (forward direction) SELECT DISTINCT     ERES.RateTypeName          AS ExchangeRateTypeCode,     ERES.RateTypeDescription   AS ExchangeRateTypeName,     'Dynamics365'             AS DataSource,     ERES.FromCurrency         AS FromCurrencyCode,     ERES.ToCurrency           AS ToCurrencyCode,     ERES.StartDate            AS ValidFrom,     ERES.EndDate              AS ValidTo,     ERES.EXCHANGERATE / 100   AS ExchangeRate FROM `mycatalog`.`DataStore`.`SMRBIExchangeRateStaging` ERES CROSS JOIN  (SELECT groupCurrencyCode FROM `mycatalog`.`ETL`.`GroupCurrency` LIMIT 1) gc  UNION ALL  -- 2️⃣ From normal exchange rates (reverse direction) SELECT DISTINCT     ERES.RateTypeName          AS ExchangeRateTypeCode,     ERES.RateTypeDescription   AS ExchangeRateTypeName,     'Dynamics365'             AS DataSource,     ERES.ToCurrency           AS FromCurrencyCode,     ERES.FromCurrency         AS ToCurrencyCode,     ERES.StartDate            AS ValidFrom,     ERES.EndDate              AS ValidTo,     1 / (ERES.EXCHANGERATE / 100) AS ExchangeRate FROM `mycatalog`.`DataStore`.`SMRBIExchangeRateStaging` ERES CROSS JOIN  (SELECT groupCurrencyCode FROM `mycatalog`.`ETL`.`GroupCurrency` LIMIT 1) gc  UNION ALL  -- 3️⃣ Synthetic rows that assign the rate 1 when the source currency matches itself SELECT DISTINCT     ERES.RateTypeName          AS ExchangeRateTypeCode,     ERES.RateTypeDescription   AS ExchangeRateTypeName,     'Dynamics365'             AS DataSource,     ERES.FromCurrency         AS FromCurrencyCode,     ERES.FromCurrency         AS ToCurrencyCode,     cast('1900-01-01' AS timestamp) AS ValidFrom,     cast('9999-12-31' AS timestamp) AS ValidTo,     1 AS ExchangeRate FROM `mycatalog`.`DataStore`.`SMRBIExchangeRateStaging` ERES CROSS JOIN  (SELECT groupCurrencyCode FROM `mycatalog`.`ETL`.`GroupCurrency` LIMIT 1) gc  UNION ALL  -- 4️⃣ Synthetic rows that assign the rate 1 when the target currency matches itself SELECT DISTINCT     ERES.RateTypeName          AS ExchangeRateTypeCode,     ERES.RateTypeDescription   AS ExchangeRateTypeName,     'Dynamics365'             AS DataSource,     ERES.ToCurrency           AS FromCurrencyCode,     ERES.ToCurrency           AS ToCurrencyCode,     cast('1900-01-01' AS timestamp) AS ValidFrom,     cast('9999-12-31' AS timestamp) AS ValidTo,     1 AS ExchangeRate FROM `mycatalog`.`DataStore`.`SMRBIExchangeRateStaging` ERES CROSS JOIN  (SELECT groupCurrencyCode FROM `mycatalog`.`ETL`.`GroupCurrency` LIMIT 1) gc;
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
