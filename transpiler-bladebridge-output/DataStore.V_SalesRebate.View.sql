/****** Object:  View [DataStore].[V_SalesRebate]    Script Date: 03/03/2026 16:26:09 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_SalesRebate` AS


SELECT	UPPER(RT0.DataAreaId) AS CompanyCode
	  , RT0.PdsRebateId AS SalesRebateCode
	  , RT0.SalesInvoiceId AS SalesInvoiceCode
	  , RT0.CustInvoiceTransRefRecId AS SalesInvoiceLineId
	  , COALESCE(NULLIF(UPPER(RT0.ItemId), ''), '_N/A') AS ProductCode
	  , RT0.CustAccount AS RebateCustomerCode
	  , RT0.CurrencyCode AS RebateCurrencyCode
	  , RT0.PdsStartingRebateAmt AS RebateAmountOriginal
	  , SUM(COALESCE(RT1.PdsCorrectedRebateAmt, 0)) AS RebateAmountCompleted
	  , SUM(COALESCE(RT2.PdsCorrectedRebateAmt, 0)) AS RebateAmountMarked
	  , SUM(COALESCE(RT3.PdsCorrectedRebateAmt, 0)) AS RebateAmountCancelled
	  , CASE WHEN COALESCE(RT1.PdsCorrectedRebateAmt, 0) = 0 
		  THEN 0 
		  ELSE SUM(COALESCE(RT1.PdsCorrectedRebateAmt, 0) - COALESCE(RT0.PdsStartingRebateAmt, 0)) 
		END AS RebateAmountVariance
				--RebateAmountVariance is the difference between the original rebate amount and the completed rebate amount(s), only when the corrected rebate amount is not 0.
				--Variances can come from upward adjusted rebates (/C larger than the original), downward adjusted rebates (cancelled amounts) or rebates that haven't been completed yet
FROM 
	(SELECT	DISTINCT 
			  CASE WHEN PdsRebateId like '%/C%' THEN LTRIM(RTRIM(LEFT(PdsRebateId, INSTR(PdsRebateId, '/C') -1)))
				   ELSE PdsRebateId 
			  END AS PdsRebateId 
			, CurrencyCode
			, CustAccount
			, DataAreaId
			, ItemId
			, SalesInvoiceId
			, CustInvoiceTransRefRecId
			, PdsStartingRebateAmt
	FROM dbo.SMRBIPdsRebateTableStaging) RT0

LEFT JOIN dbo.SMRBIPdsRebateTableStaging RT1
ON RT0.PdsRebateId = CASE WHEN RT1.PdsRebateId like '%/C%' THEN LTRIM(RTRIM(LEFT(RT1.PdsRebateId, INSTR(RT1.PdsRebateId, '/C') -1))) ELSE RT1.PdsRebateId END
AND RT0.DataAreaId = RT1.DataAreaId
AND RT0.SalesInvoiceId = RT1.SalesInvoiceId
AND RT0.ItemId = RT1.ItemId
AND RT0.CustInvoiceTransRefRecId = RT1.CustInvoiceTransRefRecId
AND RT1.PdsRebateStatus = 5 --Processed

LEFT JOIN dbo.SMRBIPdsRebateTableStaging RT2
ON RT0.PdsRebateId = CASE WHEN RT2.PdsRebateId like '%/C%' THEN LTRIM(RTRIM(LEFT(RT2.PdsRebateId, INSTR(RT2.PdsRebateId, '/C') -1))) ELSE RT2.PdsRebateId END
AND RT0.DataAreaId = RT2.DataAreaId
AND RT0.SalesInvoiceId = RT2.SalesInvoiceId
AND RT0.ItemId = RT2.ItemId
AND RT0.CustInvoiceTransRefRecId = RT2.CustInvoiceTransRefRecId
AND RT2.PdsRebateStatus = 6 --Marked

LEFT JOIN dbo.SMRBIPdsRebateTableStaging RT3
ON RT0.PdsRebateId = CASE WHEN RT3.PdsRebateId like '%/C%' THEN LTRIM(RTRIM(LEFT(RT3.PdsRebateId, INSTR(RT3.PdsRebateId, '/C') -1))) ELSE RT3.PdsRebateId END
AND RT0.DataAreaId = RT3.DataAreaId
AND RT0.SalesInvoiceId = RT3.SalesInvoiceId
AND RT0.ItemId = RT3.ItemId
AND RT0.CustInvoiceTransRefRecId = RT3.CustInvoiceTransRefRecId
AND RT3.PdsRebateStatus = 9 --Cancelled

WHERE 1=1

GROUP BY  RT0.DataAreaId
		, RT0.PdsRebateId
		, RT0.SalesInvoiceId
		, RT0.ItemId
		, RT0.CustAccount
		, RT0.CurrencyCode
		, RT0.PdsStartingRebateAmt
		, RT1.PdsCorrectedRebateAmt
		, RT0.CustInvoiceTransRefRecId
;
