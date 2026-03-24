/****** Object:  View [DWH].[V_DimBusinessOwner]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimBusinessOwner` AS 


SELECT	  CustomerId AS BusinessOwnerId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(CustomerCode) AS BusinessOwnerCode
		, CustomerName AS BusinessOwnerName
		, CustomerCodeName AS BusinessOwnerCodeName
		, CustomerGroup as BusinessOwnerGroup
		, CustomerGroupName AS BusinessOwnerGroupName
		, CustomerGroupCodeName AS BusinessOwnerGroupCodeName
		, CustomerClass AS BusinessOwnerClass	
		, CustomerClassName AS BusinessOwnerClassName
		, CustomerClassCodeName AS BusinessOwnerClassCodeName
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
