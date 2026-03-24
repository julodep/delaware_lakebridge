/****** Object:  View [DWH].[V_DIO2]    Script Date: 03/03/2026 16:26:09 ******/





CREATE OR REPLACE VIEW `DWH`.`V_DIO2` AS 


SELECT 
'This view is implemented in SSIS!' AS Comment

/*

/*************************************************************************************************************************************************************************************************************************************

Purpose: In this view, the actual DIO is calculated, according to the COUNTBACK DIO CALCULATION method

	Giving more weight to the current month’s sales, it reflects the correct assumption that most of the Stock balance will be from current, as opposed to previous sales.
	It also takes into account the real effect of the actual difference in the number of days per month (i.e. 28 in February vs. 30 in April, June, September, November vs. 31 the rest of the months).

		Formula:
	
				Prior Periods Inventory = Month end net A/R balance - Current month’s sales 

				(If the prior period’s inventory is larger than the prior month’s sales, repeat step 1. The DIO will be greater than 2 months.)

				Prior Period = (Prior Period's Inventory / Credit Sales for Prior Period) x Number Days in Period

*************************************************************************************************************************************************************************************************************************************/


--Step 1: Countback difference to 0
;
WITH TempDIO AS (

	SELECT	t1.DimCompanyId,
			t1.DimProductId,
			t1.MonthId,
			t1.CalendarDays,
			SUM(t2.SalesVolume) AS SumSalesVolume,
			SUM(t2.SalesValue) AS SumSalesValue

	FROM TMP.DIO1 t1

	INNER JOIN TMP.DIO1 t2
	ON t1.MonthId <=t2.MonthId
		AND t1.DimCompanyId = t2.DimCompanyId
		AND t1.DimProductId = t2.DimProductId

	WHERE 1=1
		and t2.MonthId <= 201711

	GROUP BY 
	t1.DimCompanyId,
	t1.DimProductId,
	t1.MonthId,
	t1.CalendarDays
),

TempDIO2 AS (

	SELECT 	t1.DimCompanyId,
			t1.DimProductId,
			t1.MonthId,
			t1.SalesVolume,
			t1.SalesValue

	FROM TMP.DIO1 t1

	WHERE 1=1
		and t1.MonthId <= 201711
),

TempDIO3 AS (

	SELECT	t1.DimCompanyId,
				t1.DimProductId,
				t1.MonthId,
				t1.CalendarDays,
				CalendarDaysCalc = t4.CalendarDays,
				MonthId2 = t4.MonthId,
				t5.SalesVolume,
				t5.SalesValue,
				t1.StockVolume,
				t1.StockValue, 
				DiffVolume = t1.StockVolume - t4.SumSalesVolume,
				DiffValue = t1.StockValue - t4.SumSalesValue

		FROM TMP.DIO1 t1

		INNER JOIN TempDIO t4
		ON t1.DimCompanyId = t4.DimCompanyId
		AND t1.DimProductId = t4.DimProductId
		AND t1.MonthId >= t4.MonthId

		LEFT JOIN TempDIO2 t5
		ON t1.DimCompanyId = t5.DimCompanyId
		AND t1.DimProductId = t5.DimProductId
		AND t4.MonthId = t5.MonthId

		WHERE 1=1
			and t1.MonthId = 201711

),

--Based on the difference, calculate the number of calendar days to count back
TempDIO4 AS (

	SELECT	t6.DimCompanyId,
			t6.DimProductId,
			t6.MonthId,
			t6.MonthId2,
			t6.CalendarDays,
			CB_DIO_StockVolume = t6.StockVolume,
			CB_DIO_StockValue = t6.StockValue,
			CB_DIO_SalesToInventoryVolume = CASE WHEN t6.DiffVolume >= 0 OR (t6.DiffVolume < 0 and t7.DiffVolume >= 0) THEN t6.SalesVolume ELSE 0 END,
			CB_DIO_SalesToInventoryValue = CASE WHEN t6.DiffValue >= 0 OR (t6.DiffValue < 0 and t7.DiffValue >= 0) THEN t6.SalesValue ELSE 0 END,
			CB_DIO_CountBackVolume = CASE WHEN t6.DiffVolume = 0 and t6.StockVolume = 0 and t6.SalesVolume = 0 THEN 0--All balances are 0
									WHEN t6.DiffVolume > 0 THEN t6.CalendarDaysCalc --For full months, take the total Calendar Days 
									WHEN t6.DiffVolume <= 0 and t7.DiffVolume > 0 THEN COALESCE(t6.CalendarDaysCalc * t7.DiffVolume / NULLIF(t6.SalesVolume, 0), 0)
									--Else, calculate DIO as Period Calendar Days x Period Net Inventory Balance / Period Sales
									ELSE 0
									END,
			CB_DIO_CountBackValue = CASE WHEN t6.DiffValue = 0 and t6.StockValue = 0 and t6.SalesValue = 0 THEN 0--All balances are 0
									WHEN t6.DiffValue > 0 THEN t6.CalendarDaysCalc --For full months, take the total Calendar Days 
									WHEN t6.DiffValue <= 0 and t7.DiffValue > 0 THEN COALESCE(t6.CalendarDaysCalc * t7.DiffValue / NULLIF(t6.SalesValue, 0), 0)
									--Else, calculate DIO as Period Calendar Days x Period Net Inventory Balance / Period Sales
									ELSE 0
									END

	FROM TempDIO3 t6

	LEFT JOIN TempDIO3 t7
	ON t6.DimCompanyId = t7.DimCompanyId 
	AND t6.DimProductId = t7.DimProductId
	AND CAST(CAST(t6.MonthId2 AS STRING) || '01' AS date) = DATEADD(MONTH, -1, (CAST(CAST(t7.MonthId2 AS STRING) || '01' AS date)))

	--ORDER BY t6.DimCompanyId, t6.DimProductId, t6.MonthId2 --> Temporary code: REMOVE !!!

)

--SUM CalendarDays to construct the single DIO per month and Load into TMP.DIO2
--INSERT INTO TMP.DIO2
SELECT	MonthId,
		CalendarDays,
		DimCompanyId,
		DimProductId,
		DIO_Volume_CountBack = SUM(CB_DIO_CountBackVolume),
		DIO_Volume_Standard = 0,
		DIO_Value_CountBack = SUM(CB_DIO_CountBackValue),
		DIO_Value_Standard = 0
FROM TempDIO4 t8

GROUP BY MonthId,
		CalendarDays,
		DimCompanyId,
		DimProductId


*/
;
