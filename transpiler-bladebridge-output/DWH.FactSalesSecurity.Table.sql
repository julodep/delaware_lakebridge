/****** Object:  Table [DWH].[FactSalesSecurity]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactSalesSecurity`(
	`FactBISalesSecurityId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimSystemUserId` int NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactBISalesSecurityId` PRIMARY KEY CLUSTERED 
(
	`FactBISalesSecurityId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactBISalesSecurity_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactBISalesSecurity_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactBISalesSecurity_DimSystemUser` FOREIGN KEY(`DimSystemUserId`)
REFERENCES `DWH`.`DimSystemUser` (`DimSystemUserId`)
;
