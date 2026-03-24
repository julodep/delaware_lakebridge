/****** Object:  View [DataStore].[V_Employee]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_Employee` AS 

/* Cost Center is a financial dimension --> Alter/Copy where required */
;
SELECT	HCMWorkerRecId AS EmployeeId
	  , PersonnelNumber AS EmployeeCode
	  , COALESCE(NULLIF(`Name`, ''), '_N/A') AS EmployeeName
	  , PersonnelNumber || ' ' || COALESCE(NULLIF(`Name`, ''), '_N/A') AS EmployeeCodeName
	  , 'Werknemer' AS DimensionName

FROM dbo.SMRBIHcmWorkerStaging
;
