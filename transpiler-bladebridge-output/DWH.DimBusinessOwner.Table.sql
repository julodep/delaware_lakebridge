/****** Object:  Table [DWH].[DimBusinessOwner]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimBusinessOwner`(
	`DimBusinessOwnerId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`BusinessOwnerId` bigint NOT NULL,
	`BusinessOwnerCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`BusinessOwnerGroup`  STRING NOT NULL,
	`SalesGroup`  STRING NOT NULL,
	`BusinessOwnerGroupName`  STRING NOT NULL,
	`BusinessOwnerGroupCodeName`  STRING NOT NULL,
	`BusinessOwnerClass`  STRING NOT NULL,
	`BusinessOwnerClassName`  STRING NOT NULL,
	`BusinessOwnerClassCodeName`  STRING NOT NULL,
	`Agent`  STRING NOT NULL,
	`BusinessOwnerName`  STRING NOT NULL,
	`BusinessOwnerCodeName`  STRING NOT NULL,
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
	`TaxGroup`  STRING,
 CONSTRAINT `PK_DimBusinessOwner` PRIMARY KEY CLUSTERED 
(
	`DimBusinessOwnerId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
