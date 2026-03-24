DECLARE VARIABLE V_JoinClause STRING;
/****** Object:  UserDefinedFunction [ETL].[F_GetTargetLookupJoinClause]    Script Date: 03/03/2026 16:26:08 ******/




CREATE FUNCTION `ETL`.`F_GetTargetLookupJoinClause` (V_TargetLookups ETL.TargetLookup READONLY, V_LookupTableAlias STRING)
RETURNS STRING
AS

	
WITH SourceJoinColumns AS
	(
		SELECT 
			TL.LookupTableAlias,
			SplittedSource.Value,
			ROW_NUMBER() OVER (ORDER BY TL.LookupTableAlias) AS RowNum
		FROM V_TargetLookups AS TL
		CROSS APPLY ETL.F_SplitString(TL.SourceJoinColumns,',') AS SplittedSource
		WHERE TL.LookupTableAlias = V_LookupTableAlias
	),
	LookupJoinColumns AS
	(
		SELECT 
			TL.LookupTableAlias,
			SplittedLookup.Value,
			ROW_NUMBER() OVER (ORDER BY TL.LookupTableAlias) AS RowNum
		FROM V_TargetLookups AS TL
		CROSS APPLY ETL.F_SplitString(TL.LookupJoinColumns,',') AS SplittedLookup
		WHERE TL.LookupTableAlias = V_LookupTableAlias
	)
	SET VARIABLE V_JoinClause = (SELECT  COALESCE(V_JoinClause || ' AND ', '') ||
		'Source.' || S.Value || '=' || S.LookupTableAlias || '.' || L.Value
	FROM SourceJoinColumns S
	INNER JOIN LookupJoinColumns L
		ON L.LookupTableAlias = S.LookupTableAlias
		AND L.RowNum = S.RowNum

	 limit 1);
RETURN V_JoinClause
;
