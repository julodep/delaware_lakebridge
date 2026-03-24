/****** Object:  Table [DataStore].[CostCenter]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`CostCenter`(
	`CostCenterId` bigint NOT NULL,
	`CostCenterCode`  STRING NOT NULL,
	`CostCenterName`  STRING NOT NULL,
	`CostCenterCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
