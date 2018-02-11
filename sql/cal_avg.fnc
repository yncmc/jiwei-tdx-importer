CREATE OR REPLACE FUNCTION cal_avg(pCode IN VARCHAR2, pEndId INT, d IN INT) RETURN NUMBER IS
    RESULT NUMBER;
    i INT;
    vFromId INT := pEndId - d + 1;
BEGIN

    SELECT COUNT(*) INTO i FROM day_trades t WHERE t.code = pCode AND t.id = vFromId;
    IF i = 0 THEN
        RETURN 0;
    END IF;

    SELECT SUM(t.closepx)/d INTO RESULT
    FROM day_trades t
    WHERE t.code = pCode
        AND t.id BETWEEN vFromId AND pEndId;
    RETURN(RESULT);
END;
/
