# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Customer.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Customer.View.sql`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Customer` AS
SELECT
    -- 1. Customer identifiers
    CCS.CustomerRecId                           AS CustomerId,
    upper(CCS.DataAreaId)                       AS CompanyCode,

    -- 2. Customer codes – upper‑case and default value
    nvl(upper(CCS.CustomerAccount), '_N/A')     AS CustomerCode,

    -- 3. Customer name, with empty string treated as NULL
    nvl(nullif(CCS.Name, ''), '_N/A')           AS CustomerName,

    -- 4. Combined code + name (NULL‑safety)
    upper(CCS.CustomerAccount) || '-' || nvl(CCS.Name, '_N/A') AS CustomerCodeName,

    -- 5. Customer group logic
    nvl(nullif(CCS.CustomerGroupId, ''), '_N/A')       AS CustomerGroup,
    nvl(CCGES.NAME, '_N/A')                           AS CustomerGroupName,
    nvl(
        CASE WHEN CCS.CustomerGroupId = '' THEN '_N/A'
             WHEN CCGES.NAME IS NULL THEN '_N/A'
             ELSE CCS.CustomerGroupId || ' - ' || CCGES.NAME
        END,
        '_N/A'
    )                                                   AS CustomerGroupCodeName,

    -- 6. Classification logic
    nvl(nullif(CCS.CustClassificationId, ''), '_N/A')   AS CustomerClass,
    nvl(nullif(CPCGS.DESCRIPTION, ''), '_N/A')          AS CustomerClassName,
    nvl(
        CASE WHEN CCS.CustClassificationId = '' THEN '_N/A'
             WHEN CPCGS.DESCRIPTION IS NULL THEN '_N/A'
             ELSE CCS.CustClassificationId || ' - ' || CPCGS.DESCRIPTION
        END,
        '_N/A'
    )                                                   AS CustomerClassCodeName,

    -- 7. Address & location details
    cast(nvl(nullif(CCS.Address, ''), '_N/A') as STRING) AS Address,
    nvl(nullif(CCS.ZipCode, ''), '_N/A')                      AS PostalCode,
    nvl(nullif(CCS.City, ''), '_N/A')                         AS City,
    nvl(nullif(TRANSL.CountryRegionId, ''), '_N/A')           AS Country,

    -- 8. Sales‑related fields (empty strings are replaced by _N/A)
    nvl(nullif(CCS.CommissionSalesGroupId, ''), '_N/A')   AS SalesGroup,
    nvl(nullif(CCS.CommissionSalesGroupId, ''), '_N/A')   AS Agent,

    -- 9. Sales responsible – data from the HCM worker staging
    nvl(HWS1.PersonnelNumber, '_N/A')   AS SalesResponsibleCode,
    nvl(HWS1.Name, '_N/A')               AS SalesResponsibleName,

    -- 10. Segment codes
    nvl(nullif(CCS.SalesSegmentId, ''), '_N/A')   AS SalesSegmentCode,
    nvl(nullif(CCS.SalesSubSegmentId, ''), '_N/A') AS SalesSubSegmentCode,

    -- 11. Delivery terms & on‑hold status
    nvl(nullif(CCS.DlvTerm, ''), '_N/A')                          AS DeliveryTerms,
    cast(nvl(nullif(ESM.Name, ''), '_N/A') as STRING)             AS OnholdStatus,

    -- 12. Credit‑related flags
    CASE WHEN CCS.CreditLimitIsMandatory = 0 THEN 'No' ELSE 'Yes' END AS CreditLimitIsMandatory,
    nvl(CCS.CreditLimit, 0)    AS CreditLimit,

    -- 13. Company chain & tax group – defaulting to _N/A
    nvl(nullif(CCS.YSLECompanyChainID, ''), 'N_A')                  AS CompanyChain,
    nvl(nullif(CC.SALESTAXGROUP, ''), 'N_A')                         AS TaxGroup
FROM `dbe_dbx_internships`.`dbo`.`SMRBICustomerStaging` CCS

    -- 1️⃣ Normalisation of ODS mapping – handled in the view body
    -- ----------------------------------------------------------------

    LEFT JOIN (
        SELECT DISTINCT CountryRegionId, ShortName
        FROM `dbe_dbx_internships`.`dbo`.`SMRBILogisticsAddressCountryRegionTranslationStaging`
        WHERE LanguageId = 'en-us'
    ) TRANSL
    ON CCS.CountryRegionId = TRANSL.CountryRegionId

    LEFT JOIN `dbe_dbx_internships`.`dbo`.`SMRBICustCustomerGroupStaging` CCGES
    ON CCS.CustomerGroupId = CCGES.CustGroup
    AND CCS.DataAreaId = CCGES.DataAreaId

    LEFT JOIN `dbe_dbx_internships`.`dbo`.`SMRBICustomerPriorityClassificationGroupStaging` CPCGS
    ON CCS.DataAreaId = CPCGS.DataAreaId
    AND CCS.CustClassificationId = CPCGS.CustomerPriorityClassificationGroupCode

    LEFT JOIN (
        SELECT DISTINCT *
        FROM `dbe_dbx_internships`.`dbo`.`SMRBIHcmWorkerStaging`
    ) HWS1
    ON CCS.MainContactWorker = HWS1.HcmWorkerRecId

    LEFT JOIN `dbe_dbx_internships`.`ETL`.`StringMap` ESM
    ON ESM.SourceTable = 'CustCustomerV2staging'
    AND ESM.SourceColumn = 'OnHoldStatus'
    AND ESM.Enum = CCS.OnHoldStatus

    LEFT JOIN `dbe_dbx_internships`.`dbo`.`CustCustomerV3Staging` CC
    ON CCS.DATAAREAID = CC.DATAAREAID
    AND CCS.CUSTOMERACCOUNT = CC.CUSTOMERACCOUNT;
""")

# COMMAND ----------

print("View `V_Customer` has been created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 4421)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_Customer` AS SELECT     -- 1. Customer identifiers     CCS.CustomerRecId                           AS CustomerId,     upper(CCS.DataAreaId)                       AS CompanyCode,      -- 2. Customer codes – upper‑case and default value     nvl(upper(CCS.CustomerAccount), '_N/A')     AS CustomerCode,      -- 3. Customer name, with empty string treated as NULL     nvl(nullif(CCS.Name, ''), '_N/A')           AS CustomerName,      -- 4. Combined code + name (NULL‑safety)     upper(CCS.CustomerAccount) || '-' || nvl(CCS.Name, '_N/A') AS CustomerCodeName,      -- 5. Customer group logic     nvl(nullif(CCS.CustomerGroupId, ''), '_N/A')       AS CustomerGroup,     nvl(CCGES.NAME, '_N/A')                           AS CustomerGroupName,     nvl(         CASE WHEN CCS.CustomerGroupId = '' THEN '_N/A'              WHEN CCGES.NAME IS NULL THEN '_N/A'              ELSE CCS.CustomerGroupId || ' - ' || CCGES.NAME         END,         '_N/A'     )                                                   AS CustomerGroupCodeName,      -- 6. Classification logic     nvl(nullif(CCS.CustClassificationId, ''), '_N/A')   AS CustomerClass,     nvl(nullif(CPCGS.DESCRIPTION, ''), '_N/A')          AS CustomerClassName,     nvl(         CASE WHEN CCS.CustClassificationId = '' THEN '_N/A'              WHEN CPCGS.DESCRIPTION IS NULL THEN '_N/A'              ELSE CCS.CustClassificationId || ' - ' || CPCGS.DESCRIPTION         END,         '_N/A'     )                                                   AS CustomerClassCodeName,      -- 7. Address & location details     cast(nvl(nullif(CCS.Address, ''), '_N/A') as STRING) AS Address,     nvl(nullif(CCS.ZipCode, ''), '_N/A')                      AS PostalCode,     nvl(nullif(CCS.City, ''), '_N/A')                         AS City,     nvl(nullif(TRANSL.CountryRegionId, ''), '_N/A')           AS Country,      -- 8. Sales‑related fields (empty strings are replaced by _N/A)     nvl(nullif(CCS.CommissionSalesGroupId, ''), '_N/A')   AS SalesGroup,     nvl(nullif(CCS.CommissionSalesGroupId, ''), '_N/A')   AS Agent,      -- 9. Sales responsible – data from the HCM worker staging     nvl(HWS1.PersonnelNumber, '_N/A')   AS SalesResponsibleCode,     nvl(HWS1.Name, '_N/A')               AS SalesResponsibleName,      -- 10. Segment codes     nvl(nullif(CCS.SalesSegmentId, ''), '_N/A')   AS SalesSegmentCode,     nvl(nullif(CCS.SalesSubSegmentId, ''), '_N/A') AS SalesSubSegmentCode,      -- 11. Delivery terms & on‑hold status     nvl(nullif(CCS.DlvTerm, ''), '_N/A')                          AS DeliveryTerms,     cast(nvl(nullif(ESM.Name, ''), '_N/A') as STRING)             AS OnholdStatus,      -- 12. Credit‑related flags     CASE WHEN CCS.CreditLimitIsMandatory = 0 THEN 'No' ELSE 'Yes' END AS CreditLimitIsMandatory,     nvl(CCS.CreditLimit, 0)    AS CreditLimit,      -- 13. Company chain & tax group – defaulting to _N/A     nvl(nullif(CCS.YSLECompanyChainID, ''), 'N_A')                  AS CompanyChain,     nvl(nullif(CC.SALESTAXGROUP, ''), 'N_A')                         AS TaxGroup FROM `_placeholder_`.`dbo`.`SMRBICustomerStaging` CCS      -- 1️⃣ Normalisation of ODS mapping – handled in the view body     -- ----------------------------------------------------------------      LEFT JOIN (         SELECT DISTINCT CountryRegionId, ShortName         FROM `_placeholder_`.`dbo`.`SMRBILogisticsAddressCountryRegionTranslationStaging`         WHERE LanguageId = 'en-us'     ) TRANSL     ON CCS.CountryRegionId = TRANSL.CountryRegionId      LEFT JOIN `_placeholder_`.`dbo`.`SMRBICustCustomerGroupStaging` CCGES     ON CCS.CustomerGroupId = CCGES.CustGroup     AND CCS.DataAreaId = CCGES.DataAreaId      LEFT JOIN `_placeholder_`.`dbo`.`SMRBICustomerPriorityClassificationGroupStaging` CPCGS     ON CCS.DataAreaId = CPCGS.DataAreaId     AND CCS.CustClassificationId = CPCGS.CustomerPriorityClassificationGroupCode      LEFT JOIN (         SELECT DISTINCT *         FROM `_placeholder_`.`dbo`.`SMRBIHcmWorkerStaging`     ) HWS1     ON CCS.MainContactWorker = HWS1.HcmWorkerRecId      LEFT JOIN `_placeholder_`.`ETL`.`StringMap` ESM     ON ESM.SourceTable = 'CustCustomerV2staging'     AND ESM.SourceColumn = 'OnHoldStatus'     AND ESM.Enum = CCS.OnHoldStatus      LEFT JOIN `_placeholder_`.`dbo`.`CustCustomerV3Staging` CC     ON CCS.DATAAREAID = CC.DATAAREAID     AND CCS.CUSTOMERACCOUNT = CC.CUSTOMERACCOUNT;
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
