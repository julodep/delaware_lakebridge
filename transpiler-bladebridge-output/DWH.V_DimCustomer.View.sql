/****** Object:  View [DWH].[V_DimCustomer]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DWH`.`V_DimCustomer` AS 


SELECT	  CustomerId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(CustomerCode) AS CustomerCode
		, CustomerName
		, CustomerCodeName
		, CustomerGroup
		, CustomerGroupName
		, CustomerGroupCodeName
		, CustomerClass
		, CustomerClassName
		, CustomerClassCodeName
		, `Address`
		, PostalCode
		, City
		, Country
		, SalesGroup
		, Agent
		, SalesResponsibleCode
		, SalesResponsibleName
		, SalesSegmentCode -- Industry Vertical in Cube
		, SalesSubSegmentCode -- Industry Classification in Cube
		, DeliveryTerms
		, OnholdStatus
		, CreditLimitIsMandatory
		, CreditLimit
		, CompanyChain
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
		, '_N/A'
		, null
		, null
		, null
		, null
		, '_N/A'

FROM DataStore.Company

;
