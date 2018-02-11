CREATE OR REPLACE FUNCTION ema12(pCode IN VARCHAR2, pId INT) RETURN NUMBER IS
    RESULT NUMBER;
    
    vScale CONSTANT NUMBER := 2/13;
    vScale2 CONSTANT NUMBER := (1 - 2/13);
    
    vTrade day_trades%ROWTYPE;
    vPreviousTrade day_trades%ROWTYPE;
BEGIN
    SELECT t.* INTO vTrade FROM day_trades t WHERE t.code = pCode AND t.id = pId;
    
    IF pId = 1 THEN
        RETURN vTrade.Closepx;
    END IF;
    
    SELECT t.* INTO vPreviousTrade FROM day_trades t WHERE t.code = pCode AND t.id = pId - 1;
    
    RESULT := vScale * vTrade.Closepx + vScale2 * vPreviousTrade.ema12;
    RETURN(RESULT);
END;
/
