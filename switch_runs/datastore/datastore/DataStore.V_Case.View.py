# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Case.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Case.View.sql`

# COMMAND ----------

# Create or replace the view
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Case` AS

WITH
  -- -------------------------------------------------------------------
  -- 1.  Authorised users (mimics the original `SELECT DISTINCT DGO`)
  -- -------------------------------------------------------------------
  CurrentDGO AS (
    SELECT DISTINCT DGO
    FROM `dbe_dbx_internships`.`datastore`.`dwh.factreportuserpermissions`
    WHERE UserPrincipalName = current_user()
  ),

  -- -------------------------------------------------------------------
  -- 2.  Normalised ODS mapping for the Allocation table
  -- -------------------------------------------------------------------
  DataMapped AS (
    SELECT
      F.*,
      UPPER(TRIM(
        CASE F.ODSSource
          WHEN 'fluv' THEN 'FLUVIUS'
          WHEN 'sibe' THEN 'SIBELGA'
          WHEN 'ores' THEN 'ORES'
          ELSE F.ODSSource
        END
      )) AS ODSSource_Normalized
    FROM `dbe_dbx_internships`.`datastore`.`Allocation` F
  ),

  -- -------------------------------------------------------------------
  -- 3.  Temporary table 1 – concatenate CaseRecId and EntityType
  -- -------------------------------------------------------------------
  TEMP1 AS (
    SELECT
      CONCAT_WS('', CAST(CaseRecId AS STRING), CAST(EntityType AS STRING)) AS CaseRecID_EntityType,
      *
    FROM `dbe_dbx_internships`.`datastore`.`SMRBICaseAssociationStaging`
  ),

  -- -------------------------------------------------------------------
  -- 4.  Temporary table 2 – join with all staging tables
  -- -------------------------------------------------------------------
  TEMP2 AS (
    SELECT
      T.*,
      CASE WHEN T.EntityType = '5' THEN V.VendorAccountNumber ELSE '_N/A' END AS SupplierCode,
      COALESCE(V.VendordCreatedDateTime, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS CreatedDateTimeVendor,
      CASE WHEN T.EntityType = '8' THEN SO.SalesOrderNumber ELSE '_N/A' END AS SalesOrderId,
      COALESCE(SO.SalesTableCreatedDateTime, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS CreatedDateTimeSalesOrder,
      CASE WHEN T.EntityType = '9' THEN PO.PurchaseOrderNumber ELSE '_N/A' END AS PurchaseOrderId,
      COALESCE(PO.PURCHTABLECREATEDDATETIME, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS CreatedDateTimePurchaseOrder,
      CASE WHEN T.EntityType = '4' THEN C.CustomerAccount ELSE '_N/A' END AS CustomerCode,
      CAST('1900-01-01 00:00:00.000' AS TIMESTAMP) AS CreatedDateTimeCustomer,
      CASE WHEN T.EntityType = '11' THEN I.ItemNumber ELSE '_N/A' END AS ProductCode,
      CAST('1900-01-01 00:00:00.000' AS TIMESTAMP) AS CreatedDateTimeProduct
    FROM TEMP1 T
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBISalesOrderHeaderStaging` AS SO
      ON T.RefRecId = SO.SalesTableRecId AND T.EntityType = '8'
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIPurchPurchaseOrderHeaderStaging` AS PO
      ON T.RefRecId = PO.PurchTableRecId AND T.EntityType = '9'
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBICustomerStaging` AS C
      ON T.RefRecId = C.CustomerRecId AND T.EntityType = '4'
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIVendorStaging` AS V
      ON T.RefRecId = V.VendTableRecId AND T.EntityType = '5'
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResReleasedProductStaging` AS I
      ON T.RefRecId = I.ProductRecId AND T.EntityType = '11'
  ),

  -- -------------------------------------------------------------------
  -- 5.  Rank rows so that the primary record comes first
  -- -------------------------------------------------------------------
  TEMP3 AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY CaseRecID_EntityType
        ORDER BY
          IsPrimary DESC,
          CreatedDateTimeVendor,
          CreatedDateTimeSalesOrder,
          CreatedDateTimePurchaseOrder,
          CreatedDateTimeCustomer,
          CreatedDateTimeProduct
      ) AS ROWNUMBER
    FROM TEMP2
  ),

  -- -------------------------------------------------------------------
  -- 6.  Keep only the primary row per CaseRecID_EntityType
  -- -------------------------------------------------------------------
  TEMP4 AS (
    SELECT CaseRecId, EntityType, RefRecId
    FROM TEMP3
    WHERE ROWNUMBER = 1
  ),

  -- -------------------------------------------------------------------
  -- 7.  Pivot the 1‑27 EntityType columns into a wide format
  -- -------------------------------------------------------------------
  TEMP5 AS (
    SELECT
      CaseRecId,
      MAX(CASE WHEN EntityType = '1'  THEN RefRecId END) AS EntityType1,
      MAX(CASE WHEN EntityType = '2'  THEN RefRecId END) AS EntityType2,
      MAX(CASE WHEN EntityType = '3'  THEN RefRecId END) AS EntityType3,
      MAX(CASE WHEN EntityType = '4'  THEN RefRecId END) AS EntityType4,
      MAX(CASE WHEN EntityType = '5'  THEN RefRecId END) AS EntityType5,
      MAX(CASE WHEN EntityType = '6'  THEN RefRecId END) AS EntityType6,
      MAX(CASE WHEN EntityType = '7'  THEN RefRecId END) AS EntityType7,
      MAX(CASE WHEN EntityType = '8'  THEN RefRecId END) AS EntityType8,
      MAX(CASE WHEN EntityType = '9'  THEN RefRecId END) AS EntityType9,
      MAX(CASE WHEN EntityType = '10' THEN RefRecId END) AS EntityType10,
      MAX(CASE WHEN EntityType = '11' THEN RefRecId END) AS EntityType11,
      MAX(CASE WHEN EntityType = '12' THEN RefRecId END) AS EntityType12,
      MAX(CASE WHEN EntityType = '13' THEN RefRecId END) AS EntityType13,
      MAX(CASE WHEN EntityType = '14' THEN RefRecId END) AS EntityType14,
      MAX(CASE WHEN EntityType = '15' THEN RefRecId END) AS EntityType15,
      MAX(CASE WHEN EntityType = '16' THEN RefRecId END) AS EntityType16,
      MAX(CASE WHEN EntityType = '17' THEN RefRecId END) AS EntityType17,
      MAX(CASE WHEN EntityType = '18' THEN RefRecId END) AS EntityType18,
      MAX(CASE WHEN EntityType = '19' THEN RefRecId END) AS EntityType19,
      MAX(CASE WHEN EntityType = '20' THEN RefRecId END) AS EntityType20,
      MAX(CASE WHEN EntityType = '21' THEN RefRecId END) AS EntityType21,
      MAX(CASE WHEN EntityType = '22' THEN RefRecId END) AS EntityType22,
      MAX(CASE WHEN EntityType = '23' THEN RefRecId END) AS EntityType23,
      MAX(CASE WHEN EntityType = '24' THEN RefRecId END) AS EntityType24,
      MAX(CASE WHEN EntityType = '25' THEN RefRecId END) AS EntityType25,
      MAX(CASE WHEN EntityType = '26' THEN RefRecId END) AS EntityType26,
      MAX(CASE WHEN EntityType = '27' THEN RefRecId END) AS EntityType27
    FROM TEMP4
    GROUP BY CaseRecId
  )

-- --------------------------------------------------------------------
-- Final SELECT – build the V_Case output using the pre‑processed data
-- --------------------------------------------------------------------
SELECT
  NVL(NULLIF(C.CASEID, ''), '_N/A')               AS CaseCode,
  NVL(NULLIF(C.DataAreaId, ''), '_N/A')           AS CompanyCode,
  NVL(NULLIF(V.VendorAccountNumber, ''), '_N/A')  AS SupplierCode,
  NVL(NULLIF(SO.SalesOrderNumber, ''), '_N/A')    AS SalesOrderCode,
  NVL(NULLIF(PO.PurchaseOrderNumber, ''), '_N/A') AS PurchaseOrderCode,
  NVL(NULLIF(CU.CustomerAccount, ''), '_N/A')     AS CustomerCode,
  NVL(NULLIF(P.ItemNumber, ''), '_N/A')           AS ProductCode,
  CAST(COALESCE(C.CaseDetailCreatedDateTime, CAST('1900-01-01' AS DATE)) AS DATE) AS CreatedDateTime,
  NVL(NULLIF(C.CaseDetailCreatedBy, ''), '_N/A')  AS CreatedBy,
  CAST(COALESCE(C.ClosedDateTime, CAST('1900-01-01' AS DATE)) AS DATE) AS ClosedDateTime,
  NVL(NULLIF(C.ClosedBy, ''), '_N/A')             AS ClosedBy,
  NVL(NULLIF(C.Description, ''), '_N/A')          AS Description,
  NVL(NULLIF(C.Memo, ''), '_N/A')                AS Memo,
  NVL(NULLIF(W.Name, ''), '_N/A')                AS OwnerWorker,
  NVL(NULLIF(C.Priority, ''), '_N/A')           AS Priority,
  NVL(NULLIF(C.Process, ''), '_N/A')             AS Process,
  CAST(COALESCE(StringMapCaseStatus.Name, '_N/A') AS STRING) AS Status,
  CAST(COALESCE(C.PlannedEffectiveDate, CAST('1900-01-01' AS DATE)) AS DATE) AS PlannedEffectiveDate,
  COALESCE(C.CaseCategoryRecId, 0)                AS CaseCategoryRecId,
  NVL(NULLIF(CCH.CaseCategory, ''), '_N/A')       AS CaseCategoryName,
  CAST(COALESCE(CCH.Description, '_N/A') AS STRING) AS CaseCategoryDescription,
  CAST(COALESCE(SM.Name, '_N/A') AS STRING)        AS CaseCategoryType,
  NVL(NULLIF(CCH.Process, ''), '_N/A')            AS CaseCategoryProcess
FROM `dbe_dbx_internships`.`datastore`.`SMRBICaseDetailBaseStaging` AS C
LEFT JOIN TEMP5 AS CA
  ON C.CaseDetailBaseRecId = CA.CaseRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBICaseCategoryHierarchyDetailStaging` AS CCH
  ON C.CategoryRecId = CCH.CaseCategoryHierarchyRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBISalesOrderHeaderStaging` AS SO
  ON CA.EntityType8 = SO.SalesTableRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIHcmWorkerStaging` AS W
  ON W.HcmWorkerRecId = C.OwnerWorker
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIPurchPurchaseOrderHeaderStaging` AS PO
  ON CA.EntityType9 = PO.PurchTableRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBICustomerStaging` AS CU
  ON CA.EntityType4 = CU.CustomerRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIVendorStaging` AS V
  ON CA.EntityType5 = V.VendTableRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResReleasedProductStaging` AS P
  ON CA.EntityType11 = P.ProductRecId
LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` AS StringMapCaseStatus
  ON StringMapCaseStatus.SourceTable = 'CaseStatus'
  AND StringMapCaseStatus.Enum = CAST(C.STATUS AS STRING)
LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` AS SM
  ON SM.SourceTable = 'CaseCategoryType'
  AND SM.Enum = CAST(C.CaseCategoryType AS STRING);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 9704)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_Case` AS  WITH   -- -------------------------------------------------------------------   -- 1.  Authorised users (mimics the original `SELECT DISTINCT DGO`)   -- -------------------------------------------------------------------   CurrentDGO AS (     SELECT DISTINCT DGO     FROM `_placeholder_`.`_placeholder_`.`dwh.factreportuserpermissions`     WHERE UserPrincipalName = current_user()   ),    -- -------------------------------------------------------------------   -- 2.  Normalised ODS mapping for the Allocation table   -- -------------------------------------------------------------------   DataMapped AS (     SELECT       F.*,       UPPER(TRIM(         CASE F.ODSSource           WHEN 'fluv' THEN 'FLUVIUS'           WHEN 'sibe' THEN 'SIBELGA'           WHEN 'ores' THEN 'ORES'           ELSE F.ODSSource         END       )) AS ODSSource_Normalized     FROM `_placeholder_`.`_placeholder_`.`Allocation` F   ),    -- -------------------------------------------------------------------   -- 3.  Temporary table 1 – concatenate CaseRecId and EntityType   -- -------------------------------------------------------------------   TEMP1 AS (     SELECT       CONCAT_WS('', CAST(CaseRecId AS STRING), CAST(EntityType AS STRING)) AS CaseRecID_EntityType,       *     FROM `_placeholder_`.`_placeholder_`.`SMRBICaseAssociationStaging`   ),    -- -------------------------------------------------------------------   -- 4.  Temporary table 2 – join with all staging tables   -- -------------------------------------------------------------------   TEMP2 AS (     SELECT       T.*,       CASE WHEN T.EntityType = '5' THEN V.VendorAccountNumber ELSE '_N/A' END AS SupplierCode,       COALESCE(V.VendordCreatedDateTime, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS CreatedDateTimeVendor,       CASE WHEN T.EntityType = '8' THEN SO.SalesOrderNumber ELSE '_N/A' END AS SalesOrderId,       COALESCE(SO.SalesTableCreatedDateTime, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS CreatedDateTimeSalesOrder,       CASE WHEN T.EntityType = '9' THEN PO.PurchaseOrderNumber ELSE '_N/A' END AS PurchaseOrderId,       COALESCE(PO.PURCHTABLECREATEDDATETIME, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS CreatedDateTimePurchaseOrder,       CASE WHEN T.EntityType = '4' THEN C.CustomerAccount ELSE '_N/A' END AS CustomerCode,       CAST('1900-01-01 00:00:00.000' AS TIMESTAMP) AS CreatedDateTimeCustomer,       CASE WHEN T.EntityType = '11' THEN I.ItemNumber ELSE '_N/A' END AS ProductCode,       CAST('1900-01-01 00:00:00.000' AS TIMESTAMP) AS CreatedDateTimeProduct     FROM TEMP1 T     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBISalesOrderHeaderStaging` AS SO       ON T.RefRecId = SO.SalesTableRecId AND T.EntityType = '8'     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIPurchPurchaseOrderHeaderStaging` AS PO       ON T.RefRecId = PO.PurchTableRecId AND T.EntityType = '9'     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBICustomerStaging` AS C       ON T.RefRecId = C.CustomerRecId AND T.EntityType = '4'     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIVendorStaging` AS V       ON T.RefRecId = V.VendTableRecId AND T.EntityType = '5'     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIEcoResReleasedProductStaging` AS I       ON T.RefRecId = I.ProductRecId AND T.EntityType = '11'   ),    -- -------------------------------------------------------------------   -- 5.  Rank rows so that the primary record comes first   -- -------------------------------------------------------------------   TEMP3 AS (     SELECT       *,       ROW_NUMBER() OVER (         PARTITION BY CaseRecID_EntityType         ORDER BY           IsPrimary DESC,           CreatedDateTimeVendor,           CreatedDateTimeSalesOrder,           CreatedDateTimePurchaseOrder,           CreatedDateTimeCustomer,           CreatedDateTimeProduct       ) AS ROWNUMBER     FROM TEMP2   ),    -- -------------------------------------------------------------------   -- 6.  Keep only the primary row per CaseRecID_EntityType   -- -------------------------------------------------------------------   TEMP4 AS (     SELECT CaseRecId, EntityType, RefRecId     FROM TEMP3     WHERE ROWNUMBER = 1   ),    -- -------------------------------------------------------------------   -- 7.  Pivot the 1‑27 EntityType columns into a wide format   -- -------------------------------------------------------------------   TEMP5 AS (     SELECT       CaseRecId,       MAX(CASE WHEN EntityType = '1'  THEN RefRecId END) AS EntityType1,       MAX(CASE WHEN EntityType = '2'  THEN RefRecId END) AS EntityType2,       MAX(CASE WHEN EntityType = '3'  THEN RefRecId END) AS EntityType3,       MAX(CASE WHEN EntityType = '4'  THEN RefRecId END) AS EntityType4,       MAX(CASE WHEN EntityType = '5'  THEN RefRecId END) AS EntityType5,       MAX(CASE WHEN EntityType = '6'  THEN RefRecId END) AS EntityType6,       MAX(CASE WHEN EntityType = '7'  THEN RefRecId END) AS EntityType7,       MAX(CASE WHEN EntityType = '8'  THEN RefRecId END) AS EntityType8,       MAX(CASE WHEN EntityType = '9'  THEN RefRecId END) AS EntityType9,       MAX(CASE WHEN EntityType = '10' THEN RefRecId END) AS EntityType10,       MAX(CASE WHEN EntityType = '11' THEN RefRecId END) AS EntityType11,       MAX(CASE WHEN EntityType = '12' THEN RefRecId END) AS EntityType12,       MAX(CASE WHEN EntityType = '13' THEN RefRecId END) AS EntityType13,       MAX(CASE WHEN EntityType = '14' THEN RefRecId END) AS EntityType14,       MAX(CASE WHEN EntityType = '15' THEN RefRecId END) AS EntityType15,       MAX(CASE WHEN EntityType = '16' THEN RefRecId END) AS EntityType16,       MAX(CASE WHEN EntityType = '17' THEN RefRecId END) AS EntityType17,       MAX(CASE WHEN EntityType = '18' THEN RefRecId END) AS EntityType18,       MAX(CASE WHEN EntityType = '19' THEN RefRecId END) AS EntityType19,       MAX(CASE WHEN EntityType = '20' THEN RefRecId END) AS EntityType20,       MAX(CASE WHEN EntityType = '21' THEN RefRecId END) AS EntityType21,       MAX(CASE WHEN EntityType = '22' THEN RefRecId END) AS EntityType22,       MAX(CASE WHEN EntityType = '23' THEN RefRecId END) AS EntityType23,       MAX(CASE WHEN EntityType = '24' THEN RefRecId END) AS EntityType24,       MAX(CASE WHEN EntityType = '25' THEN RefRecId END) AS EntityType25,       MAX(CASE WHEN EntityType = '26' THEN RefRecId END) AS EntityType26,       MAX(CASE WHEN EntityType = '27' THEN RefRecId END) AS EntityType27     FROM TEMP4     GROUP BY CaseRecId   )  -- -------------------------------------------------------------------- -- Final SELECT – build the V_Case output using the pre‑processed data -- -------------------------------------------------------------------- SELECT   NVL(NULLIF(C.CASEID, ''), '_N/A')               AS CaseCode,   NVL(NULLIF(C.DataAreaId, ''), '_N/A')           AS CompanyCode,   NVL(NULLIF(V.VendorAccountNumber, ''), '_N/A')  AS SupplierCode,   NVL(NULLIF(SO.SalesOrderNumber, ''), '_N/A')    AS SalesOrderCode,   NVL(NULLIF(PO.PurchaseOrderNumber, ''), '_N/A') AS PurchaseOrderCode,   NVL(NULLIF(CU.CustomerAccount, ''), '_N/A')     AS CustomerCode,   NVL(NULLIF(P.ItemNumber, ''), '_N/A')           AS ProductCode,   CAST(COALESCE(C.CaseDetailCreatedDateTime, CAST('1900-01-01' AS DATE)) AS DATE) AS CreatedDateTime,   NVL(NULLIF(C.CaseDetailCreatedBy, ''), '_N/A')  AS CreatedBy,   CAST(COALESCE(C.ClosedDateTime, CAST('1900-01-01' AS DATE)) AS DATE) AS ClosedDateTime,   NVL(NULLIF(C.ClosedBy, ''), '_N/A')             AS ClosedBy,   NVL(NULLIF(C.Description, ''), '_N/A')          AS Description,   NVL(NULLIF(C.Memo, ''), '_N/A')                AS Memo,   NVL(NULLIF(W.Name, ''), '_N/A')                AS OwnerWorker,   NVL(NULLIF(C.Priority, ''), '_N/A')           AS Priority,   NVL(NULLIF(C.Process, ''), '_N/A')             AS Process,   CAST(COALESCE(StringMapCaseStatus.Name, '_N/A') AS STRING) AS Status,   CAST(COALESCE(C.PlannedEffectiveDate, CAST('1900-01-01' AS DATE)) AS DATE) AS PlannedEffectiveDate,   COALESCE(C.CaseCategoryRecId, 0)                AS CaseCategoryRecId,   NVL(NULLIF(CCH.CaseCategory, ''), '_N/A')       AS CaseCategoryName,   CAST(COALESCE(CCH.Description, '_N/A') AS STRING) AS CaseCategoryDescription,   CAST(COALESCE(SM.Name, '_N/A') AS STRING)        AS CaseCategoryType,   NVL(NULLIF(CCH.Process, ''), '_N/A')            AS CaseCategoryProcess FROM `_placeholder_`.`_placeholder_`.`SMRBICaseDetailBaseStaging` AS C LEFT JOIN TEMP5 AS CA   ON C.CaseDetailBaseRecId = CA.CaseRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBICaseCategoryHierarchyDetailStaging` AS CCH   ON C.CategoryRecId = CCH.CaseCategoryHierarchyRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBISalesOrderHeaderStaging` AS SO   ON CA.EntityType8 = SO.SalesTableRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIHcmWorkerStaging` AS W   ON W.HcmWorkerRecId = C.OwnerWorker LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIPurchPurchaseOrderHeaderStaging` AS PO   ON CA.EntityType9 = PO.PurchTableRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBICustomerStaging` AS CU   ON CA.EntityType4 = CU.CustomerRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIVendorStaging` AS V   ON CA.EntityType5 = V.VendTableRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIEcoResReleasedProductStaging` AS P   ON CA.EntityType11 = P.ProductRecId LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` AS StringMapCaseStatus   ON StringMapCaseStatus.SourceTable = 'CaseStatus'   AND StringMapCaseStatus.Enum = CAST(C.STATUS AS STRING) LEFT JOIN `_placeholder_`.`_placeholder_`.`StringMap` AS SM   ON SM.SourceTable = 'CaseCategoryType'   AND SM.Enum = CAST(C.CaseCategoryType AS STRING);
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
