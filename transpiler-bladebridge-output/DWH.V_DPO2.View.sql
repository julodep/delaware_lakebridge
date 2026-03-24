/****** Object:  View [DWH].[V_DPO2]    Script Date: 03/03/2026 16:26:09 ******/





CREATE OR REPLACE VIEW `DWH`.`V_DPO2` AS 


SELECT 
'This view is implemented in SSIS!' AS Comment

/*

/*************************************************************************************************************************************************************************************************************************************

Purpose: In this view, the actual DPO is calculated, according to the COUNTBACK DPO CALCULATION method

	Giving more weight to the current month’s purchase, it reflects the correct assumption that most of the A/P balance will be from current, as opposed to previous purchase.
	It also takes into account the real effect of the actual difference in the number of days per month (i.e. 28 in February vs. 30 in April, June, September, November vs. 31 the rest of the months).

		Formula:
	
				Prior Periods Payables = Month end net A/P balance - Current month’s purchase 

				(If the prior period’s payables is larger than the prior month’s purchase, repeat step 1. The DPO will be greater than 2 months.)

				Prior Period = (Prior Period's Payables / Credit Purchase for Prior Period) x Number Days in Period

*************************************************************************************************************************************************************************************************************************************/


--Step 1: Countback difference to 0
;
WITH TempDPO AS (

	SELECT	t1.DimCompanyId,
			t1.DimSupplierId,
			t1.MonthId,
			t1.CalendarDays,
			SUM(t2.Turnover) as SumTurnover

	FROM TMP.DPO1 t1

	INNER JOIN TMP.DPO1 t2
	ON t1.MonthId <=t2.MonthId
		AND t1.DimCompanyId = t2.DimCompanyId
		AND t1.DimSupplierId = t2.DimSupplierId

	WHERE 1=1
		and t2.MonthId <= 201702

	GROUP BY 
	t1.DimCompanyId,
	t1.DimSupplierId,
	t1.MonthId,
	t1.CalendarDays
),

TempDPO2 AS (

	SELECT 	t1.DimCompanyId,
			t1.DimSupplierId,
			t1.MonthId,
			t1.Turnover

	FROM TMP.DPO1 t1

	WHERE 1=1
		and t1.MonthId <= 201702
),

TempDPO3 AS (

	SELECT	t1.DimCompanyId,
				t1.DimSupplierId,
				t1.MonthId,
				t1.CalendarDays,
				CalendarDaysCalc = t4.CalendarDays,
				MonthId2 = t4.MonthId,
				t5.Turnover,
				t1.OpenAmount, 
				Diff = t1.OpenAmount - t4.SumTurnover

		FROM TMP.DPO1 t1

		INNER JOIN TempDPO t4
		ON t1.DimCompanyId = t4.DimCompanyId
		AND t1.DimSupplierId = t4.DimSupplierId
		AND t1.MonthId >= t4.MonthId

		LEFT JOIN TempDPO2 t5
		ON t1.DimCompanyId = t5.DimCompanyId
		AND t1.DimSupplierId = t5.DimSupplierId
		AND t4.MonthId = t5.MonthId

		WHERE 1=1
			and t1.MonthId = 201702

),

--Based on the difference, calculate the number of calendar days to count back
TempDPO4 AS (

	SELECT	t6.DimCompanyId,
			t6.DimSupplierId,
			t6.MonthId,
			t6.MonthId2,
			t6.CalendarDays,
			CB_DPO_OpenAmount = t6.OpenAmount,
			CB_DPO_PurchaseToPayables = CASE WHEN t6.Diff >= 0 OR (t6.Diff < 0 and t7.Diff >= 0) THEN t6.Turnover ELSE 0 END,
			CB_DPO_CountBack = CASE WHEN t6.Diff = 0 and t6.OpenAmount = 0 and t6.Turnover = 0 THEN 0--All balances are 0
									WHEN t6.Diff > 0 THEN t6.CalendarDaysCalc --For full months, take the total Calendar Days 
									WHEN t6.Diff <= 0 and t7.Diff > 0 THEN COALESCE(t6.CalendarDaysCalc * t7.Diff / NULLIF(t6.Turnover, 0), 0)
									--Else, calculate DPO as Period Calendar Days x Period Net A/P Balance / Period Purchase
									ELSE 0
									END
	FROM TempDPO3 t6

	LEFT JOIN TempDPO3 t7
	ON t6.DimCompanyId = t7.DimCompanyId 
	AND t6.DimSupplierId = t7.DimSupplierId
	AND CAST(CAST(t6.MonthId2 AS STRING) || '01' AS date) = DATEADD(MONTH, -1, (CAST(CAST(t7.MonthId2 AS STRING) || '01' AS date)))

	--ORDER BY t6.DimCompanyId, t6.DimSupplierId, t6.MonthId2 --> Temporary code: REMOVE !!!

)

--SUM CalendarDays to construct the single DPO per month and Load into TMP.DPO2
;
INSERT INTO TMP.DPO2
SELECT	MonthId,
		CalendarDays,
		DimCompanyId,
		DimSupplierId,
		DPO_CountBack = SUM(CB_DPO_CountBack),
		DPO_Standard = 0

FROM TempDPO4 t8

GROUP BY MonthId,
		CalendarDays,
		DimCompanyId,
		DimSupplierId
*/
;
