/****** Object:  View [DWH].[V_DSO2]    Script Date: 03/03/2026 16:26:09 ******/





CREATE OR REPLACE VIEW `DWH`.`V_DSO2` AS 


SELECT 
'This view is implemented in SSIS!' AS Comment

/*

/*************************************************************************************************************************************************************************************************************************************

Purpose: In this view, the actual DSO is calculated, according to the COUNTBACK DSO CALCULATION method

	Giving more weight to the current month’s sales, it reflects the correct assumption that most of the A/R balance will be from current, as opposed to previous sales.
	It also takes into account the real effect of the actual difference in the number of days per month (i.e. 28 in February vs. 30 in April, June, September, November vs. 31 the rest of the months).

		Formula:
	
				Prior Periods Receivables = Month end net A/R balance - Current month’s sales 

				(If the prior period’s receivables is larger than the prior month’s sales, repeat step 1. The DSO will be greater than 2 months.)

				Prior Period = (Prior Period's Receivables / Credit Sales for Prior Period) x Number Days in Period

*************************************************************************************************************************************************************************************************************************************/


--Step 1: Countback difference to 0
;
WITH TempDSO AS (

	SELECT	t1.DimCompanyId,
			t1.DimCustomerId,
			t1.MonthId,
			t1.CalendarDays,
			SUM(t2.Turnover) as SumTurnover

	FROM TMP.DSO1 t1

	INNER JOIN TMP.DSO1 t2
	ON t1.MonthId <=t2.MonthId
		AND t1.DimCompanyId = t2.DimCompanyId
		AND t1.DimCustomerId = t2.DimCustomerId

	WHERE 1=1
		and t2.MonthId <= 201702

	GROUP BY 
	t1.DimCompanyId,
	t1.DimCustomerId,
	t1.MonthId,
	t1.CalendarDays
),

TempDSO2 AS (

	SELECT 	t1.DimCompanyId,
			t1.DimCustomerId,
			t1.MonthId,
			t1.Turnover

	FROM TMP.DSO1 t1

	WHERE 1=1
		and t1.MonthId <= 201702
),

TempDSO3 AS (

	SELECT	t1.DimCompanyId,
				t1.DimCustomerId,
				t1.MonthId,
				t1.CalendarDays,
				CalendarDaysCalc = t4.CalendarDays,
				MonthId2 = t4.MonthId,
				t5.Turnover,
				t1.OpenAmount, 
				Diff = t1.OpenAmount - t4.SumTurnover

		FROM TMP.DSO1 t1

		INNER JOIN TempDSO t4
		ON t1.DimCompanyId = t4.DimCompanyId
		AND t1.DimCustomerId = t4.DimCustomerId
		AND t1.MonthId >= t4.MonthId

		LEFT JOIN TempDSO2 t5
		ON t1.DimCompanyId = t5.DimCompanyId
		AND t1.DimCustomerId = t5.DimCustomerId
		AND t4.MonthId = t5.MonthId

		WHERE 1=1
			and t1.MonthId = 201702

),

--Based on the difference, calculate the number of calendar days to count back
TempDSO4 AS (

	SELECT	t6.DimCompanyId,
			t6.DimCustomerId,
			t6.MonthId,
			t6.MonthId2,
			t6.CalendarDays,
			CB_DSO_OpenAmount = t6.OpenAmount,
			CB_DSO_SalesToReceivables = CASE WHEN t6.Diff >= 0 OR (t6.Diff < 0 and t7.Diff >= 0) THEN t6.Turnover ELSE 0 END,
			CB_DSO_CountBack = CASE WHEN t6.Diff = 0 and t6.OpenAmount = 0 and t6.Turnover = 0 THEN 0--All balances are 0
									WHEN t6.Diff > 0 THEN t6.CalendarDaysCalc --For full months, take the total Calendar Days 
									WHEN t6.Diff <= 0 and t7.Diff > 0 THEN COALESCE(t6.CalendarDaysCalc * t7.Diff / NULLIF(t6.Turnover, 0), 0)
									--Else, calculate DSO as Period Calendar Days x Period Net A/R Balance / Period Sales
									ELSE 0
									END
	FROM TempDSO3 t6

	LEFT JOIN TempDSO3 t7
	ON t6.DimCompanyId = t7.DimCompanyId 
	AND t6.DimCustomerId = t7.DimCustomerId
	AND CAST(CAST(t6.MonthId2 AS STRING) || '01' AS date) = DATEADD(MONTH, -1, (CAST(CAST(t7.MonthId2 AS STRING) || '01' AS date)))

	--ORDER BY t6.DimCompanyId, t6.DimCustomerId, t6.MonthId2 --> Temporary code: REMOVE !!!

)

--SUM CalendarDays to construct the single DSO per month and Load into TMP.DSO2
;
INSERT INTO TMP.DSO2
SELECT	MonthId,
		CalendarDays,
		DimCompanyId,
		DimCustomerId,
		DSO_CountBack = SUM(CB_DSO_CountBack),
		DSO_Standard = 0

FROM TempDSO4 t8

GROUP BY MonthId,
		CalendarDays,
		DimCompanyId,
		DimCustomerId
*/
;
