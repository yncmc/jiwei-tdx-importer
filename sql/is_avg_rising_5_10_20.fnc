CREATE OR REPLACE FUNCTION is_avg_rising_5_10_20(pRowId ROWID) RETURN NUMBER
IS
    RESULT NUMBER := 0;
    vTrade day_trades%ROWTYPE;
BEGIN
    SELECT t.* INTO vTrade FROM day_trades t WHERE t.rowid = pRowid;
    
    IF vTrade.Avg5 >= vTrade.Avg10 AND vTrade.Avg10 >= vTrade.Avg20 THEN
        RESULT := 1;
    END IF;
    
    RETURN(RESULT);
END;
/
