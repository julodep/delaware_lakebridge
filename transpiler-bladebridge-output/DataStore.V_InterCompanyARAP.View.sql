/****** Object:  View [DataStore].[V_InterCompanyARAP]    Script Date: 03/03/2026 16:26:09 ******/








/****** Script for SelectTopNRows command from SSMS  ******/


CREATE OR REPLACE VIEW `DataStore`.`V_InterCompanyARAP` AS 


SELECT 
	    CAST(CAST(COALESCE(`Invoice`, '1900-01-01') AS date) AS TIMESTAMP) AS InvoiceDate
	  , YEAR(CAST(COALESCE(`Invoice`, '1900-01-01') AS timestamp)) AS YearInvoice
      , CAST(CAST(COALESCE(`Posted`, '1900-01-01') AS date) AS TIMESTAMP) AS PostedDate
      , CAST(CAST(COALESCE(`Due`, '1900-01-01') AS date) AS TIMESTAMP) AS DueDate
      , COALESCE(`Brn.`, '_N/A')  AS Brn
      , COALESCE(`Dept`, '_N/A') AS Departement
      , COALESCE(`Lgr.`, '_N/A') AS AR_AP_Type
      , COALESCE(`Type`, '_N/A') AS Type
      , CASE WHEN `Lgr.` = 'AR' THEN COALESCE(`Transaction #`, '_N/A') ELSE '_N/A' END AS SalesInvoiceCode
	  , CASE WHEN `Lgr.` = 'AP' THEN COALESCE(`Transaction #`, '_N/A')  ELSE '_N/A' END AS PurchaseInvoiceCode
      , COALESCE(`Job_Invoice_/_Posting_Ref`, '_N/A') AS JobInvoice
      , COALESCE(`Cur`, '_N/A') AS Currency
	  , COALESCE(CAST(`Invoice_Total` AS DECIMAL(38,8)), 0) AS InvoiceTotal	  
     -- , ISNULL(CONVERT(NUMERIC(20,8),[Outstanding OS Currency]		), 0) AS
    --  , ISNULL(CONVERT(NUMERIC(20,8),[Outstanding Local Equiv]		), 0) AS
      , CASE WHEN `Lgr.` = 'AR' THEN COALESCE(`Account`, '_N/A') ELSE '_N/A' END AS CustomerCode
	  , CASE WHEN `Lgr.` = 'AP' THEN COALESCE(`Account`, '_N/A') ELSE '_N/A' END AS SupplierCode
      , COALESCE(`Account_Name`, '_N/A') AS AccountName
      , COALESCE(`AP_Settlement`, '_N/A') AS AP_Settlement
      , COALESCE(`AR_Settlement`, '_N/A') AS AR_Settlement
      , COALESCE(`CR_GRP`, '_N/A') AS CrGRP
      , COALESCE(`DR_GRP`, '_N/A') AS DrGRP
      , COALESCE(`Dest._/_Disch.`, '_N/A') AS DestDisch
      , CAST(COALESCE(`ETA`, '19000101') AS timestamp) AS ETA
      , CAST(COALESCE(`ETD`, '19000101') AS timestamp) AS ETD
      , COALESCE(`House`, '_N/A') AS House
      , COALESCE(`Job_Number`, '_N/A') AS JobNumber
      , COALESCE(`Master`, '_N/A') AS `Master`
      , COALESCE(`Org._Country_`, '_N/A') AS OrigCountry
      , COALESCE(`Org._Country_Name_`, '_N/A') AS OrigCountryName
      , COALESCE(`Origin_/_Load`, '_N/A') AS OriginLoad	 
	  , CompanyCode
	  , COALESCE(CAST(Exchange AS DECIMAL(32,16)), 0) AS ExchangeRate
	  
  FROM 
  (
  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'EU10' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_EU10` A
  WHERE Invoice is not null 

  UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'BE20' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_BE20` A
  WHERE Invoice is not null 
 
   UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'BX20' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_BX20` A
  WHERE Invoice is not null
  
  UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'EM14' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_EM14` A
  WHERE Invoice is not null 
 
   UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'LU20' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_LU20` A
  WHERE Invoice is not null 

     UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'NL20' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_NL20` A
  WHERE Invoice is not null 


   UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'SE20' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_SE20` A
  WHERE Invoice is not null  

   UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'FR50' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_FR50` f
  WHERE Invoice is not null
  
     UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'DE30' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_DE30` f
  WHERE Invoice is not null

     UNION ALL

  SELECT  `Invoice`
		, `Posted`
		, `Due`
		, `Brn.`
		, `Dept`
		, `Lgr.`
		, `Type`
		, `Transaction_#`
		, `Job_Invoice_/_Posting_Ref`
		, `Cur`
		, `Invoice_Total`
		, `Exchange`
		, `Local_Total`
		, `Outstanding_OS_Currency`
		, `Outstanding_Local_Equiv`
		, `Account`
		, `Account_Name`
		, `AP_Settlement`
		, `AR_Settlement`
		, `CR_GRP`
		, `DR_GRP`
		, `Dest._/_Disch.`
		, `ETA`
		, `ETD`
		, `House`
		, `Job_Number`
		, `Master`
		, `Org._Country_`
		, `Org._Country_Name_`
		, `Origin_/_Load`
		, 'SW31' AS CompanyCode
  FROM `StagingInterCompany`.`ARAP_SW31` f
  WHERE Invoice is not null
  
  
  
  ) a


  
;
