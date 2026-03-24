/****** Object:  Table [DWH].[DimDestinationAgent]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimDestinationAgent`(
	`DimDestinationAgentId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DestinationAgentId` bigint NOT NULL,
	`DestinationAgentCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`DestinationAgentGroup`  STRING NOT NULL,
	`SalesGroup`  STRING NOT NULL,
	`DestinationAgentGroupName`  STRING NOT NULL,
	`DestinationAgentGroupCodeName`  STRING NOT NULL,
	`DestinationAgentClass`  STRING NOT NULL,
	`DestinationAgentClassName`  STRING NOT NULL,
	`DestinationAgentClassCodeName`  STRING NOT NULL,
	`Agent`  STRING NOT NULL,
	`DestinationAgentName`  STRING NOT NULL,
	`DestinationAgentCodeName`  STRING NOT NULL,
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
 CONSTRAINT `PK_DimDestinationAgent` PRIMARY KEY CLUSTERED 
(
	`DimDestinationAgentId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
