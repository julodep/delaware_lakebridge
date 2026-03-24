/****** Object:  Table [DWH].[DimCustomer]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`DimCustomer`(
	`DimCustomerId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CustomerId` bigint NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`CustomerGroup`  STRING NOT NULL,
	`SalesGroup`  STRING NOT NULL,
	`CustomerGroupName`  STRING NOT NULL,
	`CustomerGroupCodeName`  STRING NOT NULL,
	`CustomerClass`  STRING NOT NULL,
	`CustomerClassName`  STRING NOT NULL,
	`CustomerClassCodeName`  STRING NOT NULL,
	`Agent`  STRING NOT NULL,
	`CustomerName`  STRING NOT NULL,
	`CustomerCodeName`  STRING NOT NULL,
	`PostalCode`  STRING NOT NULL,
	`Address`  STRING NOT NULL,
	`City`  STRING NOT NULL,
	`Country`  STRING NOT NULL,
	`FirstOrderDate` TIMESTAMP ,
	`LastOrderDate` TIMESTAMP ,
	`DateDiffFirstLastOrderDate` INT,
	`DateDiffLastTodayOrderDate` INT,
	`DeliveryTerms`  STRING NOT NULL,
	`OnholdStatus`  STRING NOT NULL,
	`CreditLimitIsMandatory`  STRING NOT NULL,
	`CreditLimit`  DECIMAL(32,6) NOT NULL,
	`SalesResponsibleCode`  STRING NOT NULL,
	`SalesResponsibleName`  STRING NOT NULL,
	`SalesSegmentCode`  STRING NOT NULL,
	`SalesSubSegmentCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`CompanyChain`  STRING NOT NULL,
	`TaxGroup`  STRING,
 CONSTRAINT `PK_DimCustomer` PRIMARY KEY CLUSTERED 
(
	`DimCustomerId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
