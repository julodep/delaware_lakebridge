/****** Object:  View [ETL].[V_ScreenGLHistoricCSV]    Script Date: 03/03/2026 16:26:08 ******/




CREATE OR REPLACE VIEW `ETL`.`V_ScreenGLHistoricCSV`
AS

WITH Screen1
AS (
	SELECT *
	FROM etl.Screening
	WHERE ScreeningDescription LIKE '%GL Historic screen all%'
	)
	, LastScreen
AS (
	SELECT MAX(StartTime) AS LastScreenDate
		, ScreeningId
	FROM etl.ScreeningExecution
	GROUP BY ScreeningId
	)
SELECT SE.StartTime
	, S1.SchemaName
	, S1.TableName
	, S1.ColumnName
	, S1.ReferenceColumns
	, S1.ReferenceSchema
	, S1.ReferenceTable
	, CASE 
		WHEN SE.STATUS = 'Completed'
			THEN 'Successfull'
		ELSE 'Incorrect data entered in csv'
		END AS Message
FROM ETL.ScreeningExecution SE
INNER JOIN Screen1 S1
	ON S1.ScreeningId = SE.ScreeningId
LEFT JOIN LastScreen LS
	ON LS.ScreeningId = S1.ScreeningId
WHERE SE.StartTime = LS.LastScreenDate
;
