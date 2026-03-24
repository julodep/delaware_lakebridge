/****** Object:  Table [dbo].[CostSheetNodeHierarchyTable]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `dbo`.`CostSheetNodeHierarchyTable`(
	`DataAreaId`  STRING,
	`AccId` INT,
	`Level_1_Code`  STRING NOT NULL,
	`Level_1_Description`  STRING NOT NULL,
	`Level_1_CostGroupId`  STRING NOT NULL,
	`Level_2_Code`  STRING,
	`Level_2_Description`  STRING,
	`Level_2_CostGroupId`  STRING,
	`Level_3_Code`  STRING,
	`Level_3_Description`  STRING,
	`Level_3_CostGroupId`  STRING
)
;
