DECLARE VARIABLE V_pos INT
;
DECLARE VARIABLE V_piece STRING
-- Need to tack a delimiter onto the END of the input string if one doesn't exist
;
/****** Object:  UserDefinedFunction [ETL].[fn_Split]    Script Date: 03/03/2026 16:26:08 ******/







CREATE FUNCTION `ETL`.`fn_Split`
(
  V_inputString STRING
  ,V_splitCharacter STRING
)
RETURNS V_tmp TABLE
(Value STRING)
AS
  
      --DECLARE @tmp TABLE (
      --  searchColumn NVARCHAR(20))
;
IF RIGHT(RTRIM(V_inputString), 1) <> V_splitCharacter
        ;
SET V_inputString = V_inputString + V_splitCharacter

      ;
SET V_pos = REGEXP_INSTR(V_inputString, '%' || V_splitCharacter || '%')

      ;
WHILE V_pos <> 0
        THEN
SET V_piece = LEFT(V_inputString, V_pos - 1)

            -- You have a piece of data, so INSERT it, print it, do whatever you want to with it.
            --print cast(@piece as varchar(500))
;
            INSERT INTO V_tmp
            VALUES     (V_piece)

            ;
SET V_inputString = CONCAT(substring(V_inputString, 1, 1 - 1), '', substring(V_inputString, 1 + V_pos))
            ;
SET V_pos = REGEXP_INSTR(V_inputString, '%' || V_splitCharacter || '%')
        ;
END IF

      ;
SIGNAL SQLSTATE '45000';
