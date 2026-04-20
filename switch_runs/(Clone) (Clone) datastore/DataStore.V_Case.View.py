# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Case.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastoredemo/DataStore.V_Case.View.sql`

# COMMAND ----------

# Replace all non‑ASCII characters (e.g. non‑breaking hyphens U+2011) with ASCII equivalents

spark.sql("""
-- 1. Temporary CTE: combine CaseRecId and EntityType into a single key
WITH
    TEMP1 AS (
        SELECT CONCAT(CaseRecId, EntityType) AS CaseRecID_EntityType,
               *
        FROM {catalog}.{schema}.`SMRBICaseAssociationStaging`
    ),

-- 2. Enrich every row with related data from the staging tables
    TEMP2 AS (
        SELECT TEMP1.*,
               CASE WHEN TEMP1.EntityType = '5' THEN V.VendorAccountNumber ELSE '_N/A' END  AS SupplierCode,
               NVL(V.VendordCreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeVendor,
               CASE WHEN TEMP1.EntityType = '8' THEN SO.SalesOrderNumber ELSE '_N/A' END  AS SalesOrderId,
               NVL(SO.SalesTableCreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeSalesOrder,
               CASE WHEN TEMP1.EntityType = '9' THEN PO.PurchaseOrderNumber ELSE '_N/A' END AS PurchaseOrderId,
               NVL(PO.PURCHTABLECREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimePurchaseOrder,
               CASE WHEN TEMP1.EntityType = '4' THEN C.CustomerAccount ELSE '_N/A' END AS CustomerCode,
               NVL(C.CUSTOMERCREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimeCustomer,
               CASE WHEN TEMP1.EntityType = '11' THEN I.ItemNumber ELSE '_N/A' END AS ProductCode,
               NVL(I.PURCHTABLECREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimeProduct
        FROM TEMP1
        LEFT JOIN {catalog}.{schema}.`SMRBISalesOrderHeaderStaging` SO
          ON TEMP1.RefRecId = SO.SalesTableRecId      AND TEMP1.EntityType = '8'
        LEFT JOIN {catalog}.{schema}.`SMRBIPurchPurchaseOrderHeaderStaging` PO
          ON TEMP1.RefRecId = PO.PurchTableRecId       AND TEMP1.EntityType = '9'
        LEFT JOIN {catalog}.{schema}.`SMRBICustomerStaging` C
          ON TEMP1.RefRecId = C.CustomerRecId             AND TEMP1.EntityType = '4'
        LEFT JOIN {catalog}.{schema}.`SMRBIVendorStaging` V
          ON TEMP1.RefRecId = V.VendTableRecId             AND TEMP1.EntityType = '5'
        LEFT JOIN {catalog}.{schema}.`SMRBIEcoResReleasedProductStaging` I
          ON TEMP1.RefRecId = I.ProductRecId               AND TEMP1.EntityType = '11'
    ),

-- 3. Pick the “primary” row for each key by ordering on the required columns
    TEMP3 AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY CaseRecID_EntityType
                   ORDER BY IsPrimary DESC,
                            CreatedDateTimeVendor,
                            CreatedDateTimeSalesOrder,
                            CreatedDateTimePurchaseOrder,
                            CreatedDateTimeCustomer,
                            CreatedDateTimeProduct
               ) AS ROWNUMBER
        FROM TEMP2
    ),

-- 4. Keep only the first (primary) record per key
    TEMP4 AS (
        SELECT CaseRecId, EntityType, RefRecId
        FROM TEMP3
        WHERE ROWNUMBER = 1
    ),

-- 5. Pivot the remaining rows into one column per EntityType (1‑27)
    TEMP5 AS (
        SELECT CaseRecId,
               MAX(CASE WHEN EntityType = '1'  THEN RefRecId END)  AS EntityType1,
               MAX(CASE WHEN EntityType = '2'  THEN RefRecId END)  AS EntityType2,
               MAX(CASE WHEN EntityType = '3'  THEN RefRecId END)  AS EntityType3,
               MAX(CASE WHEN EntityType = '4'  THEN RefRecId END)  AS EntityType4,
               MAX(CASE WHEN EntityType = '5'  THEN RefRecId END)  AS EntityType5,
               MAX(CASE WHEN EntityType = '6'  THEN RefRecId END)  AS EntityType6,
               MAX(CASE WHEN EntityType = '7'  THEN RefRecId END)  AS EntityType7,
               MAX(CASE WHEN EntityType = '8'  THEN RefRecId END)  AS EntityType8,
               MAX(CASE WHEN EntityType = '9'  THEN RefRecId END)  AS EntityType9,
               MAX(CASE WHEN EntityType = '10' THEN RefRecId END)  AS EntityType10,
               MAX(CASE WHEN EntityType = '11' THEN RefRecId END)  AS EntityType11,
               MAX(CASE WHEN EntityType = '12' THEN RefRecId END)  AS EntityType12,
               MAX(CASE WHEN EntityType = '13' THEN RefRecId END)  AS EntityType13,
               MAX(CASE WHEN EntityType = '14' THEN RefRecId END)  AS EntityType14,
               MAX(CASE WHEN EntityType = '15' THEN RefRecId END)  AS EntityType15,
               MAX(CASE WHEN EntityType = '16' THEN RefRecId END)  AS EntityType16,
               MAX(CASE WHEN EntityType = '17' THEN RefRecId END)  AS EntityType17,
               MAX(CASE WHEN EntityType = '18' THEN RefRecId END)  AS EntityType18,
               MAX(CASE WHEN EntityType = '19' THEN RefRecId END)  AS EntityType19,
               MAX(CASE WHEN EntityType = '20' THEN RefRecId END)  AS EntityType20,
               MAX(CASE WHEN EntityType = '21' THEN RefRecId END)  AS EntityType21,
               MAX(CASE WHEN EntityType = '22' THEN RefRecId END)  AS EntityType22,
               MAX(CASE WHEN EntityType = '23' THEN RefRecId END)  AS EntityType23,
               MAX(CASE WHEN EntityType = '24' THEN RefRecId END)  AS EntityType24,
               MAX(CASE WHEN EntityType = '25' THEN RefRecId END)  AS EntityType25,
               MAX(CASE WHEN EntityType = '26' THEN RefRecId END)  AS EntityType26,
               MAX(CASE WHEN EntityType = '27' THEN RefRecId END)  AS EntityType27
        FROM TEMP4
        GROUP BY CaseRecId
    )

-- 6. Final SELECT: pull all requested columns, apply NVL/CAST and join everything
SELECT
    NVL(NULLIF(C.CASEID,''), '_N/A')           AS CaseCode,
    NVL(NULLIF(C.DataAreaId,''), '_N/A')        AS CompanyCode,
    NVL(NULLIF(V.VendorAccountNumber,''), '_N/A') AS SupplierCode,
    NVL(NULLIF(SO.SalesOrderNumber,''), '_N/A')  AS SalesOrderCode,
    NVL(NULLIF(PO.PurchaseOrderNumber,''), '_N/A') AS PurchaseOrderCode,
    NVL(NULLIF(CU.CustomerAccount,''), '_N/A')  AS CustomerCode,
    NVL(NULLIF(P.ItemNumber,''), '_N/A')        AS ProductCode,
    CAST(NVL(C.CaseDetailCreatedDateTime, '1900-01-01') AS DATE)   AS CreatedDateTime,
    NVL(NULLIF(C.CaseDetailCreatedBy,''), '_N/A')   AS CreatedBy,
    CAST(NVL(C.ClosedDateTime, '1900-01-01') AS DATE)        AS ClosedDateTime,
    NVL(NULLIF(C.ClosedBy,''), '_N/A')           AS ClosedBy,
    NVL(NULLIF(C.Description,''), '_N/A')        AS Description,
    NVL(NULLIF(C.Memo,''), '_N/A')               AS Memo,
    NVL(NULLIF(W.Name,''), '_N/A')              AS OwnerWorker,
    NVL(NULLIF(C.Priority,''), '_N/A')          AS Priority,
    NVL(NULLIF(C.Process,''), '_N/A')           AS Process,
    CAST(NVL(NULLIF(StringMapCaseStatus.Name, ''), '_N/A') AS STRING) AS Status,
    CAST(NVL(C.PlannedEffectiveDate, DATE '1900-01-01') AS DATE)  AS PlannedEffectiveDate,
    NVL(C.CaseCategoryRecId, 0)                 AS CaseCategoryRecId,
    NVL(NULLIF(CCH.CaseCategory,''), '_N/A')    AS CaseCategoryName,
    CAST(NVL(NULLIF(SM.Name, ''), '_N/A') AS STRING) AS CaseCategoryType,
    NVL(NULLIF(CCH.Description,''), '_N/A')    AS CaseCategoryDescription,
    NVL(NULLIF(CCH.Process,''), '_N/A')        AS CaseCategoryProcess
FROM {catalog}.{schema}.`SMRBICaseDetailBaseStaging` C
LEFT JOIN TEMP5 CA ON C.CaseDetailBaseRecId = CA.CaseRecId
LEFT JOIN {catalog}.{schema}.`SMRBICaseCategoryHierarchyDetailStaging` CCH
          ON C.CategoryRecId = CCH.CaseCategoryHierarchyRecId
LEFT JOIN {catalog}.{schema}.`SMRBISalesOrderHeaderStaging` SO
          ON CA.EntityType8  = SO.SalesTableRecId
LEFT JOIN {catalog}.{schema}.`SMRBIHcmWorkerStaging` W
          ON W.HcmWorkerRecId = C.OwnerWorker
LEFT JOIN {catalog}.{schema}.`SMRBIPurchPurchaseOrderHeaderStaging` PO
          ON CA.EntityType9  = PO.PurchTableRecId
LEFT JOIN {catalog}.{schema}.`SMRBICustomerStaging` CU
          ON CA.EntityType4  = CU.CustomerRecId
LEFT JOIN {catalog}.{schema}.`SMRBIVendorStaging` V
          ON CA.EntityType5  = V.VendTableRecId
LEFT JOIN {catalog}.{schema}.`SMRBIEcoResReleasedProductStaging` P
          ON CA.EntityType11 = P.ProductRecId
LEFT JOIN {catalog}.{schema}.`StringMap` StringMapCaseStatus
          ON StringMapCaseStatus.SourceTable = 'CaseStatus'
          AND StringMapCaseStatus.Enum = CAST(C.STATUS AS STRING)
LEFT JOIN {catalog}.{schema}.`StringMap` SM
          ON SM.SourceTable = 'CaseCategoryType'
          AND SM.Enum = CAST(C.CaseCategoryType AS STRING)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 8283)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- 1. Temporary CTE: combine CaseRecId and EntityType into a single key WITH     TEMP1 AS (         SELECT CONCAT(CaseRecId, EntityType) AS CaseRecID_EntityType,                *         FROM `catalog`.`schema`.`SMRBICaseAssociationStaging`     ),  -- 2. Enrich every row with related data from the staging tables     TEMP2 AS (         SELECT TEMP1.*,                CASE WHEN TEMP1.EntityType = '5' THEN V.VendorAccountNumber ELSE '_N/A' END  AS SupplierCode,                NVL(V.VendordCreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeVendor,                CASE WHEN TEMP1.EntityType = '8' THEN SO.SalesOrderNumber ELSE '_N/A' END  AS SalesOrderId,                NVL(SO.SalesTableCreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeSalesOrder,                CASE WHEN TEMP1.EntityType = '9' THEN PO.PurchaseOrderNumber ELSE '_N/A' END AS PurchaseOrderId,                NVL(PO.PURCHTABLECREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimePurchaseOrder,                CASE WHEN TEMP1.EntityType = '4' THEN C.CustomerAccount ELSE '_N/A' END AS CustomerCode,                NVL(C.CUSTOMERCREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimeCustomer,                CASE WHEN TEMP1.EntityType = '11' THEN I.ItemNumber ELSE '_N/A' END AS ProductCode,                NVL(I.PURCHTABLECREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimeProduct         FROM TEMP1         LEFT JOIN `catalog`.`schema`.`SMRBISalesOrderHeaderStaging` SO           ON TEMP1.RefRecId = SO.SalesTableRecId      AND TEMP1.EntityType = '8'         LEFT JOIN `catalog`.`schema`.`SMRBIPurchPurchaseOrderHeaderStaging` PO           ON TEMP1.RefRecId = PO.PurchTableRecId       AND TEMP1.EntityType = '9'         LEFT JOIN `catalog`.`schema`.`SMRBICustomerStaging` C           ON TEMP1.RefRecId = C.CustomerRecId             AND TEMP1.EntityType = '4'         LEFT JOIN `catalog`.`schema`.`SMRBIVendorStaging` V           ON TEMP1.RefRecId = V.VendTableRecId             AND TEMP1.EntityType = '5'         LEFT JOIN `catalog`.`schema`.`SMRBIEcoResReleasedProductStaging` I           ON TEMP1.RefRecId = I.ProductRecId               AND TEMP1.EntityType = '11'     ),  -- 3. Pick the “primary” row for each key by ordering on the required columns     TEMP3 AS (         SELECT *,                ROW_NUMBER() OVER (                    PARTITION BY CaseRecID_EntityType                    ORDER BY IsPrimary DESC,                             CreatedDateTimeVendor,                             CreatedDateTimeSalesOrder,                             CreatedDateTimePurchaseOrder,                             CreatedDateTimeCustomer,                             CreatedDateTimeProduct                ) AS ROWNUMBER         FROM TEMP2     ),  -- 4. Keep only the first (primary) record per key     TEMP4 AS (         SELECT CaseRecId, EntityType, RefRecId         FROM TEMP3         WHERE ROWNUMBER = 1     ),  -- 5. Pivot the remaining rows into one column per EntityType (1‑27)     TEMP5 AS (         SELECT CaseRecId,                MAX(CASE WHEN EntityType = '1'  THEN RefRecId END)  AS EntityType1,                MAX(CASE WHEN EntityType = '2'  THEN RefRecId END)  AS EntityType2,                MAX(CASE WHEN EntityType = '3'  THEN RefRecId END)  AS EntityType3,                MAX(CASE WHEN EntityType = '4'  THEN RefRecId END)  AS EntityType4,                MAX(CASE WHEN EntityType = '5'  THEN RefRecId END)  AS EntityType5,                MAX(CASE WHEN EntityType = '6'  THEN RefRecId END)  AS EntityType6,                MAX(CASE WHEN EntityType = '7'  THEN RefRecId END)  AS EntityType7,                MAX(CASE WHEN EntityType = '8'  THEN RefRecId END)  AS EntityType8,                MAX(CASE WHEN EntityType = '9'  THEN RefRecId END)  AS EntityType9,                MAX(CASE WHEN EntityType = '10' THEN RefRecId END)  AS EntityType10,                MAX(CASE WHEN EntityType = '11' THEN RefRecId END)  AS EntityType11,                MAX(CASE WHEN EntityType = '12' THEN RefRecId END)  AS EntityType12,                MAX(CASE WHEN EntityType = '13' THEN RefRecId END)  AS EntityType13,                MAX(CASE WHEN EntityType = '14' THEN RefRecId END)  AS EntityType14,                MAX(CASE WHEN EntityType = '15' THEN RefRecId END)  AS EntityType15,                MAX(CASE WHEN EntityType = '16' THEN RefRecId END)  AS EntityType16,                MAX(CASE WHEN EntityType = '17' THEN RefRecId END)  AS EntityType17,                MAX(CASE WHEN EntityType = '18' THEN RefRecId END)  AS EntityType18,                MAX(CASE WHEN EntityType = '19' THEN RefRecId END)  AS EntityType19,                MAX(CASE WHEN EntityType = '20' THEN RefRecId END)  AS EntityType20,                MAX(CASE WHEN EntityType = '21' THEN RefRecId END)  AS EntityType21,                MAX(CASE WHEN EntityType = '22' THEN RefRecId END)  AS EntityType22,                MAX(CASE WHEN EntityType = '23' THEN RefRecId END)  AS EntityType23,                MAX(CASE WHEN EntityType = '24' THEN RefRecId END)  AS EntityType24,                MAX(CASE WHEN EntityType = '25' THEN RefRecId END)  AS EntityType25,                MAX(CASE WHEN EntityType = '26' THEN RefRecId END)  AS EntityType26,                MAX(CASE WHEN EntityType = '27' THEN RefRecId END)  AS EntityType27         FROM TEMP4         GROUP BY CaseRecId     )  -- 6. Final SELECT: pull all requested columns, apply NVL/CAST and join everything SELECT     NVL(NULLIF(C.CASEID,''), '_N/A')           AS CaseCode,     NVL(NULLIF(C.DataAreaId,''), '_N/A')        AS CompanyCode,     NVL(NULLIF(V.VendorAccountNumber,''), '_N/A') AS SupplierCode,     NVL(NULLIF(SO.SalesOrderNumber,''), '_N/A')  AS SalesOrderCode,     NVL(NULLIF(PO.PurchaseOrderNumber,''), '_N/A') AS PurchaseOrderCode,     NVL(NULLIF(CU.CustomerAccount,''), '_N/A')  AS CustomerCode,     NVL(NULLIF(P.ItemNumber,''), '_N/A')        AS ProductCode,     CAST(NVL(C.CaseDetailCreatedDateTime, '1900-01-01') AS DATE)   AS CreatedDateTime,     NVL(NULLIF(C.CaseDetailCreatedBy,''), '_N/A')   AS CreatedBy,     CAST(NVL(C.ClosedDateTime, '1900-01-01') AS DATE)        AS ClosedDateTime,     NVL(NULLIF(C.ClosedBy,''), '_N/A')           AS ClosedBy,     NVL(NULLIF(C.Description,''), '_N/A')        AS Description,     NVL(NULLIF(C.Memo,''), '_N/A')               AS Memo,     NVL(NULLIF(W.Name,''), '_N/A')              AS OwnerWorker,     NVL(NULLIF(C.Priority,''), '_N/A')          AS Priority,     NVL(NULLIF(C.Process,''), '_N/A')           AS Process,     CAST(NVL(NULLIF(StringMapCaseStatus.Name, ''), '_N/A') AS STRING) AS Status,     CAST(NVL(C.PlannedEffectiveDate, DATE '1900-01-01') AS DATE)  AS PlannedEffectiveDate,     NVL(C.CaseCategoryRecId, 0)                 AS CaseCategoryRecId,     NVL(NULLIF(CCH.CaseCategory,''), '_N/A')    AS CaseCategoryName,     CAST(NVL(NULLIF(SM.Name, ''), '_N/A') AS STRING) AS CaseCategoryType,     NVL(NULLIF(CCH.Description,''), '_N/A')    AS CaseCategoryDescription,     NVL(NULLIF(CCH.Process,''), '_N/A')        AS CaseCategoryProcess FROM `catalog`.`schema`.`SMRBICaseDetailBaseStaging` C LEFT JOIN TEMP5 CA ON C.CaseDetailBaseRecId = CA.CaseRecId LEFT JOIN `catalog`.`schema`.`SMRBICaseCategoryHierarchyDetailStaging` CCH           ON C.CategoryRecId = CCH.CaseCategoryHierarchyRecId LEFT JOIN `catalog`.`schema`.`SMRBISalesOrderHeaderStaging` SO           ON CA.EntityType8  = SO.SalesTableRecId LEFT JOIN `catalog`.`schema`.`SMRBIHcmWorkerStaging` W           ON W.HcmWorkerRecId = C.OwnerWorker LEFT JOIN `catalog`.`schema`.`SMRBIPurchPurchaseOrderHeaderStaging` PO           ON CA.EntityType9  = PO.PurchTableRecId LEFT JOIN `catalog`.`schema`.`SMRBICustomerStaging` CU           ON CA.EntityType4  = CU.CustomerRecId LEFT JOIN `catalog`.`schema`.`SMRBIVendorStaging` V           ON CA.EntityType5  = V.VendTableRecId LEFT JOIN `catalog`.`schema`.`SMRBIEcoResReleasedProductStaging` P           ON CA.EntityType11 = P.ProductRecId LEFT JOIN `catalog`.`schema`.`StringMap` StringMapCaseStatus           ON StringMapCaseStatus.SourceTable = 'CaseStatus'           AND StringMapCaseStatus.Enum = CAST(C.STATUS AS STRING) LEFT JOIN `catalog`.`schema`.`StringMap` SM           ON SM.SourceTable = 'CaseCategoryType'           AND SM.Enum = CAST(C.CaseCategoryType AS STRING)
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
