/****** Object:  Table [DataStore].[AnalyticalDimensionLedger]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`AnalyticalDimensionLedger`(
	`LedgerDimensionId` bigint NOT NULL,
	`MainAccount` bigint NOT NULL,
	`Intercompany` bigint NOT NULL,
	`BusinessSegment` bigint NOT NULL,
	`EndCustomer` bigint NOT NULL,
	`Department` bigint NOT NULL,
	`LocalAccount` bigint NOT NULL,
	`Location` bigint NOT NULL,
	`Product` bigint NOT NULL,
	`ShipmentContract` bigint NOT NULL,
	`Vendor` bigint NOT NULL
)
;
