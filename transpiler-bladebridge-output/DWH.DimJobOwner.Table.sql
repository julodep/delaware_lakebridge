/****** Object:  Table [DWH].[DimJobOwner]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimJobOwner`(
	`DimJobOwnerId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`JobOwnerId` bigint NOT NULL,
	`JobOwnerCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`JobOwnerGroup`  STRING NOT NULL,
	`SalesGroup`  STRING NOT NULL,
	`JobOwnerGroupName`  STRING NOT NULL,
	`JobOwnerGroupCodeName`  STRING NOT NULL,
	`JobOwnerClass`  STRING NOT NULL,
	`JobOwnerClassName`  STRING NOT NULL,
	`JobOwnerClassCodeName`  STRING NOT NULL,
	`Agent`  STRING NOT NULL,
	`JobOwnerName`  STRING NOT NULL,
	`JobOwnerCodeName`  STRING NOT NULL,
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
 CONSTRAINT `PK_DimJobOwner` PRIMARY KEY CLUSTERED 
(
	`DimJobOwnerId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
