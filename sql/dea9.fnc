CREATE OR REPLACE FUNCTION dea9(pCode IN VARCHAR2, pId INT, pDif IN NUMBER) RETURN NUMBER IS
    RESULT NUMBER;
    
    vScale CONSTANT NUMBER := 2/10;
    vScale2 CONSTANT NUMBER := (1 - vScale);
    
    vTrade day_trades%ROWTYPE;
    vPreviousTrade day_trades%ROWTYPE;
BEGIN
    --dbms_output.put_line('id=' || pId);
    SELECT t.* INTO vTrade FROM day_trades t WHERE t.code = pCode AND t.id = pId;
    
    IF pId = 1 THEN
        RETURN 0;
    END IF;
    
    SELECT t.* INTO vPreviousTrade FROM day_trades t WHERE t.code = pCode AND t.id = pId - 1;
    --dbms_output.put_line('Dif:' || vTrade.Dif);
    --dbms_output.put_line('prev-Dif:' || vPreviousTrade.Dif);
    --dbms_output.put_line('prev-id:' || vPreviousTrade.id);

    RESULT := vScale * pDif + vScale2 * vPreviousTrade.Dea;
    --dbms_output.put_line('r:' || RESULT);
    RETURN(RESULT);
END;
/
