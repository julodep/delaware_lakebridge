/****** Object:  View [DWH].[V_DimDestinationAgent]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_DimDestinationAgent` AS 


SELECT	  CustomerId AS DestinationAgentId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(CustomerCode) AS DestinationAgentCode
		, CustomerName AS DestinationAgentName
		, CustomerCodeName AS DestinationAgentCodeName
		, CustomerGroup as DestinationAgentGroup
		, CustomerGroupName AS DestinationAgentGroupName
		, CustomerGroupCodeName AS DestinationAgentGroupCodeName
		, CustomerClass AS DestinationAgentClass	
		, CustomerClassName AS DestinationAgentClassName
		, CustomerClassCodeName AS DestinationAgentClassCodeName
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
