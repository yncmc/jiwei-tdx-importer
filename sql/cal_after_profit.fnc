CREATE OR REPLACE FUNCTION cal_after_profit(pCode IN VARCHAR2, pFromId INT, days INT) RETURN NUMBER
IS
    i INT;
    vToId INT := pFromId + days - 1;
    RESULT NUMBER;
BEGIN
    SELECT COUNT(*) INTO i
    FROM day_trades t
    WHERE t.code = pCode
        AND t.id = vToId;

    IF i = 0 THEN
        RETURN 0;
    END IF;

    SELECT CASE WHEN f.closepx = 0 THEN 9999 ELSE round((t.closepx - f.closepx) * 100 / f.closepx, 2) END INTO RESULT
    FROM day_trades f
    JOIN day_trades t ON t.code = f.code AND t.id = vToId
    WHERE f.code = pCode
        AND f.id = pFromId;

    IF RESULT > 999999.99 THEN
        RESULT := 999999.99;
    END IF;

    RETURN(RESULT);
END;
/
