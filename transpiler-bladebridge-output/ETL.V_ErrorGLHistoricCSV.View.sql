/****** Object:  View [ETL].[V_ErrorGLHistoricCSV]    Script Date: 03/03/2026 16:26:08 ******/




CREATE OR REPLACE VIEW `ETL`.`V_ErrorGLHistoricCSV`
AS

WITH Screen1
AS (
	SELECT *
	FROM etl.Screening
	WHERE ScreeningDescription LIKE '%GL Historic screen all%'
	)
--, LastScreen AS (
--SELECT MAX(StartTime) AS LastScreenDate, ScreeningId  from etl.ScreeningExecution group by ScreeningId) 
SELECT SE.StartTime
	, S1.SchemaName
	, S1.TableName
	, S1.ColumnName
	, SER.RowIdValues AS IncorrectValue
	, SER.Occurence
	, SER.ErrorCode
FROM ETL.ScreeningExecution SE
INNER JOIN Screen1 S1
	ON S1.ScreeningId = SE.ScreeningId
LEFT JOIN ETL.ScreeningError SER
	ON SER.ScreeningExecutionId = SE.ScreeningExecutionId
--LEFT JOIN LastScreen LS
--ON LS.ScreeningId = S1.ScreeningId
WHERE SE.STATUS <> 'Completed'
;
