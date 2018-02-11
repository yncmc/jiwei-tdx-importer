CREATE OR REPLACE FUNCTION avg_diff_5_10_20(pRowID ROWID, pDiff NUMBER) RETURN INTEGER
IS
    vTrade day_trades%ROWTYPE;    
BEGIN
    
    SELECT t.* INTO vTrade FROM day_trades t WHERE t.rowid = pRowid;
    
    IF vTrade.avg20 = 0 THEN
        RETURN 0;
    END IF;
    
    IF abs(vTrade.Avg5 - vTrade.avg20) * 100 / vTrade.avg20 > pDiff THEN 
        RETURN 0;
    END IF;

    IF abs(vTrade.Avg10 - vTrade.avg20) * 100 / vTrade.avg20 > pDiff THEN 
        RETURN 0;
    END IF;
    
    RETURN (1);
END;
/
