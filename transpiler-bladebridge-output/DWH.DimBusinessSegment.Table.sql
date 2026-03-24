/****** Object:  Table [DWH].[DimBusinessSegment]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimBusinessSegment`(
	`DimBusinessSegmentId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`BusinessSegmentId` bigint NOT NULL,
	`BusinessSegmentCode`  STRING NOT NULL,
	`BusinessSegmentName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimBusinessSegment` PRIMARY KEY CLUSTERED 
(
	`DimBusinessSegmentId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
