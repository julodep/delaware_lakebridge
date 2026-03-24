/****** Object:  Table [DWH].[DimCase]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimCase`(
	`DimCaseId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CaseCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`CreatedDateTime` TIMESTAMP NOT NULL,
	`CreatedBy`  STRING NOT NULL,
	`ClosedDateTime` TIMESTAMP NOT NULL,
	`ClosedBy`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
	`Memo`  STRING NOT NULL,
	`OwnerWorker`  STRING NOT NULL,
	`Priority`  STRING NOT NULL,
	`Process`  STRING NOT NULL,
	`Status`  STRING NOT NULL,
	`PlannedEffectiveDate` TIMESTAMP NOT NULL,
	`CaseCategoryRecId` bigint NOT NULL,
	`CaseCategoryName`  STRING NOT NULL,
	`CaseCategoryType`  STRING NOT NULL,
	`CaseCategoryDescription`  STRING NOT NULL,
	`CaseCategoryProcess`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimCase` PRIMARY KEY CLUSTERED 
(
	`DimCaseId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
