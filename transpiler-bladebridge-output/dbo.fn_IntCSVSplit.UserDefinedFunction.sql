DECLARE VARIABLE V_Iterator INT
;
DECLARE VARIABLE V_WorkString STRING
;
DECLARE VARIABLE V_FoundIndex INT
;
/****** Object:  UserDefinedFunction [dbo].[fn_IntCSVSplit]    Script Date: 03/03/2026 16:26:08 ******/



CREATE FUNCTION `dbo`.`fn_IntCSVSplit`
( V_RowData STRING )
RETURNS V_RtnValue TABLE 
( Data INT ) 
AS
 
    
SET V_Iterator = 1
    ;
SET V_FoundIndex = INSTR(V_RowData, ',')
    ;
WHILE (V_FoundIndex>0)
    DO
SET V_WorkString = LTRIM(RTRIM(SUBSTRING(V_RowData, 1, V_FoundIndex - 1)))
		;
IF CASE WHEN V_WorkString not rlike '[^0-9]' THEN 1 ELSE 0 END  = 1
		THEN
			INSERT INTO V_RtnValue 
(data) VALUES (V_WorkString)
		;
ELSE

			INSERT INTO V_RtnValue 
(data) VALUES(NULL)
		;
END WHILE;
SET V_RowData = SUBSTRING(V_RowData, V_FoundIndex + 1,LEN(V_RowData))
        ;
SET V_Iterator = V_Iterator + 1
        ;
SET V_FoundIndex = INSTR(V_RowData, ',')
    ;
END ;
IF CASE WHEN LTRIM(RTRIM(V_RowData)) not rlike '[^0-9]' THEN 1 ELSE 0 END  = 1
    THEN
        INSERT INTO V_RtnValue 
(Data) SELECT LTRIM(RTRIM(V_RowData))
    ;
END IF
    ;
SIGNAL SQLSTATE '45000';
