/****** Object:  Table [DataStore].[BusinessSegment]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`BusinessSegment`(
	`BusinessSegmentId` bigint NOT NULL,
	`BusinessSegmentCode`  STRING NOT NULL,
	`BusinessSegmentName`  STRING NOT NULL,
	`BusinessSegmentCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
