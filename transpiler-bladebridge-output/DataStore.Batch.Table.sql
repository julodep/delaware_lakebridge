/****** Object:  Table [DataStore].[Batch]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Batch`(
	`RecId` bigint NOT NULL,
	`CompanyCode`  STRING,
	`BatchCode`  STRING,
	`ProductCode`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
	`ExpiryDate` TIMESTAMP NOT NULL,
	`ProductionDate` TIMESTAMP NOT NULL
) TEXTIMAGE_ON `PRIMARY`
;
