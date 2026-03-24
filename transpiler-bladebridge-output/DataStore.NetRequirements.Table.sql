/****** Object:  Table [DataStore].[NetRequirements]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`NetRequirements`(
	`RecId` bigint NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ReferenceType`  STRING NOT NULL,
	`PlanVersion`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`RequirementDate` TIMESTAMP ,
	`RequirementTime`  STRING NOT NULL,
	`RequirementDateTime` TIMESTAMP ,
	`ReferenceCode`  STRING NOT NULL,
	`ProducedItemCode`  STRING,
	`CustomerCode`  STRING NOT NULL,
	`VendorCode`  STRING NOT NULL,
	`ActionDate` TIMESTAMP ,
	`ActionDays` int NOT NULL,
	`ActionType`  STRING NOT NULL,
	`ActionMarked`  STRING NOT NULL,
	`FuturesDate` TIMESTAMP NOT NULL,
	`FuturesDays` int NOT NULL,
	`FuturesCalculated`  STRING NOT NULL,
	`FuturesMarked`  STRING NOT NULL,
	`Direction`  STRING NOT NULL,
	`RankNr` BIGINT,
	`Quantity`  DECIMAL(32,6) NOT NULL,
	`QuantityConfirmed`  DECIMAL(32,6) NOT NULL
)
;
