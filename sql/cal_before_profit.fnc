CREATE OR REPLACE FUNCTION cal_before_profit(pCode IN VARCHAR2, pToId INT, days INT) RETURN NUMBER IS
    RESULT NUMBER;
    vFromId INTEGER := pToId - days + 1;
BEGIN
    IF vFromId < 1 THEN
       RETURN 0;
    END IF;

    --dbms_output.put_line('from:' || vFromId);
    --dbms_output.put_line('to:' || pToId);    
    SELECT CASE WHEN f.closepx = 0 THEN 9999 ELSE round((t.closepx - f.closepx) * 100 / f.closepx, 2) END INTO RESULT
    FROM day_trades f
    JOIN day_trades t ON t.code = f.code AND t.id = pToId
    WHERE f.code = pCode
        AND f.id = vFromId;
    
    IF RESULT > 999999.99 THEN
        RESULT := 999999.99;
    END IF;
    
    RETURN(RESULT);
END;
/
