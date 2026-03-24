/****** Object:  View [DataStore].[V_NetRequirements]    Script Date: 03/03/2026 16:26:09 ******/












CREATE OR REPLACE VIEW `DataStore`.`V_NetRequirements` AS


SELECT	
	--Master Data:
		RTS.ReqTransReqId AS RecId
		, COALESCE(NULLIF(RTS.DataAreaId, ''), '_N/A') AS CompanyCode
		, COALESCE(SM1.Name, '_N/A') AS ReferenceType
		, COALESCE(RPVS.ReqPlanId, '_N/A') AS PlanVersion
		, COALESCE(NULLIF(UPPER(RTS.ItemId), ''), '_N/A') AS ProductCode
		, COALESCE(NULLIF(RTS.CovInventDimId, ''), '_N/A') AS InventDimCode
		, CASE WHEN COALESCE(RTS.ReqDate, '1900-01-01') < '2005-01-01' THEN '1900-01-01' ELSE COALESCE(RTS.ReqDate, '1900-01-01') END AS RequirementDate
		, '1900-01-01 00:00:00.000' AS RequirementTime --DATEADD(second, ISNULL(RTS.ReqTime,0), '') --> Add requirement time to calculate the correct OOS during a day ! MISSING FIELD !
		, CASE WHEN COALESCE(RTS.ReqDate, '1900-01-01') < '2005-01-01' THEN '1900-01-01' ELSE RTS.ReqDate END --CASE WHEN ISNULL(RTS.ReqDate, '1900-01-01') < '2005-01-01' THEN '1900-01-01' ELSE dateadd(SECOND,ISNULL(RTS.ReqTime,0),ISNULL(RTS.ReqDate, '1900-01-01')) END --> Add requirement time to calculate the correct OOS during a day ! MISSING FIELD !
			AS RequirementDateTime

	--Reference to other dimensions:
		, COALESCE(NULLIF(RTS.RefId, ''), '_N/A') AS ReferenceCode
		, COALESCE(NULLIF(UPPER(PTS.ItemId), ''), NULLIF(UPPER(RPO.ItemId), ''), '_N/A') AS ProducedItemCode
		, COALESCE(NULLIF(SOHS.OrderingCustomerAccountNumber, ''), '_N/A') AS CustomerCode
		, COALESCE(NULLIF(POHS.OrderVendorAccountNumber, ''), '_N/A') AS VendorCode

	--Action Fields:
		, NULLIF(RTS.ActionDate, '') AS ActionDate
		, COALESCE(NULLIF(RTS.ActionDays, ''), 0) AS ActionDays
		, COALESCE(SM3.Name, '_N/A') AS ActionType
		, COALESCE(SM4.Name, '_N/A') AS ActionMarked
		, COALESCE(NULLIF(RTS.FuturesDate, ''), '') AS FuturesDate
		, COALESCE(NULLIF(RTS.FuturesDays, ''), 0) AS FuturesDays
		, COALESCE(SM6.Name, '_N/A') AS FuturesCalculated
		, COALESCE(SM5.Name, '_N/A') AS FuturesMarked
		, COALESCE(SM2.Name, '_N/A') AS Direction

	--Measures:
		, CASE WHEN RTS.RefType = 14 THEN 0 --Do not take safety stock into consideration for the calculation of the accumulated value
						ELSE ROW_NUMBER() OVER (PARTITION BY RTS.DataAreaId, RTS.CovInventDimId, RPVS.ReqPlanId, RTS.ItemId ORDER BY RTS.ReqDate ASC, RTS.Direction DESC) END AS RankNr --Create a rank for the accumulated value
		, COALESCE(RTS.Qty, 0) AS Quantity
		, CASE 
			WHEN RTS.RefType IN (31,32,33,34,43) THEN 0 --Excluding Planned Production Order, Planned Purchase Order, Planned Transfer
			ELSE COALESCE(RTS.Qty, 0) END AS QuantityConfirmed

FROM dbo.SMRBIReqTransStaging RTS

LEFT JOIN dbo.SMRBIReqPlanVersionStaging RPVS
ON RTS.DataAreaId = RPVS.ReqPlanDataAreaId
	and RTS.PlanVersion = RPVS.ReqPlanVersionRecId
	--and RPVS.Active = 1 --Only take active plans

LEFT JOIN ETL.StringMap SM1
ON SM1.SourceTable = 'SMRBIReqTransStaging'
	and SM1.SourceColumn = 'ReqRefType'
	and SM1.Enum = RTS.RefType

LEFT JOIN ETL.StringMap SM2
ON SM2.SourceTable = 'SMRBIReqTransStaging'
	and SM2.SourceColumn = 'Direction'
	and SM2.Enum = RTS.Direction

LEFT JOIN ETL.StringMap SM3
ON SM3.SourceTable = 'SMRBIReqTransStaging'
	and SM3.SourceColumn = 'ActionType'
	and SM3.Enum = RTS.ActionType

LEFT JOIN ETL.StringMap SM4
ON SM4.SourceTable = 'SMRBIReqTransStaging'
	and SM4.SourceColumn = 'ActionMarked'
	and SM4.Enum = RTS.ActionMarked

LEFT JOIN ETL.StringMap SM5
ON SM5.SourceTable = 'SMRBIReqTransStaging'
	and SM5.SourceColumn = 'FuturesMarked'
	and SM5.Enum = RTS.FuturesMarked

LEFT JOIN ETL.StringMap SM6
ON SM6.SourceTable = 'SMRBIReqTransStaging'
	and SM6.SourceColumn = 'FuturesCalculated'
	and SM6.Enum = RTS.FuturesCalculated

--Join to retrieve the reference values:
LEFT JOIN dbo.SMRBIProdTableStaging PTS
ON RTS.RefId = PTS.ProdId
	and RTS.DataAreaId = PTS.COMPANY

LEFT JOIN dbo.SMRBIPurchPurchaseOrderHeaderStaging POHS
ON RTS.RefId = POHS.PurchaseOrderNumber
	and RTS.DataAreaId = POHS.DataAreaId

LEFT JOIN dbo.SMRBISalesOrderHeaderStaging SOHS
ON RTS.RefId = SOHS.SalesOrderNumber
	and RTS.DataAreaId = SOHS.DataAreaId

LEFT JOIN dbo.SMRBIReqPOStaging RPO
ON RTS.RefId = RPO.RefId
	and RTS.DataAreaId = RPO.DataAreaId
	and RTS.PlanVersion = RPO.PlanVersion
;
