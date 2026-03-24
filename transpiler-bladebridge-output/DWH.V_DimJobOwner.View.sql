/****** Object:  View [DWH].[V_DimJobOwner]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimJobOwner` AS 


SELECT	  CustomerId AS JobOwnerId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(CustomerCode) AS JobOwnerCode
		, CustomerName AS JobOwnerName
		, CustomerCodeName AS JobOwnerCodeName
		, CustomerGroup as JobOwnerGroup
		, CustomerGroupName AS JobOwnerGroupName
		, CustomerGroupCodeName AS JobOwnerGroupCodeName
		, CustomerClass AS JobOwnerClass	
		, CustomerClassName AS JobOwnerClassName
		, CustomerClassCodeName AS JobOwnerClassCodeName
		, `Address`
		, PostalCode
		, City
		, Country
		, SalesGroup
		, Agent
		, SalesResponsibleCode
		, SalesResponsibleName
		, SalesSegmentCode
		, SalesSubSegmentCode
		, DeliveryTerms
		, OnholdStatus
		, CreditLimitIsMandatory
		, CreditLimit
		, null as FirstOrderDate
		, null as LastOrderDate
		, null as DateDiffFirstLastOrderDate
		, null as DateDiffLastTodayOrderDate
		, TaxGroup

FROM DataStore.Customer

/* Create unknown member */

UNION ALL

SELECT	DISTINCT -1
		, UPPER(CompanyCode) AS CompanyId
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, 0
		, null
		, null
		, null
		, null
		, '_N/A'

FROM DataStore.Company
;
