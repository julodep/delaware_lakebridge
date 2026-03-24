/****** Object:  Table [DataStore].[Customer]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Customer`(
	`CustomerId` bigint NOT NULL,
	`CompanyCode`  STRING,
	`CustomerCode`  STRING NOT NULL,
	`CustomerName`  STRING NOT NULL,
	`CustomerCodeName`  STRING,
	`CustomerGroup`  STRING NOT NULL,
	`CustomerGroupName`  STRING NOT NULL,
	`CustomerGroupCodeName`  STRING NOT NULL,
	`CustomerClass`  STRING NOT NULL,
	`CustomerClassName`  STRING NOT NULL,
	`CustomerClassCodeName`  STRING NOT NULL,
	`Address`  STRING,
	`PostalCode`  STRING NOT NULL,
	`City`  STRING NOT NULL,
	`Country`  STRING NOT NULL,
	`SalesGroup`  STRING NOT NULL,
	`Agent`  STRING NOT NULL,
	`SalesResponsibleCode`  STRING NOT NULL,
	`SalesResponsibleName`  STRING NOT NULL,
	`SalesSegmentCode`  STRING NOT NULL,
	`SalesSubSegmentCode`  STRING NOT NULL,
	`DeliveryTerms`  STRING NOT NULL,
	`OnholdStatus`  STRING,
	`CreditLimitIsMandatory`  STRING,
	`CreditLimit`  DECIMAL(32,6) NOT NULL,
	`CompanyChain`  STRING NOT NULL,
	`TaxGroup`  STRING NOT NULL
)
;
