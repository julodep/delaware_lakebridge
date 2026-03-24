/****** Object:  Table [DWH].[DimDeliveryTerms]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimDeliveryTerms`(
	`DimDeliveryTermsId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DeliveryTermsCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`DeliveryTermsName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimDeliveryTerms` PRIMARY KEY CLUSTERED 
(
	`DimDeliveryTermsId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
