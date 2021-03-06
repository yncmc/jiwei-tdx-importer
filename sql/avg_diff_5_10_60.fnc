CREATE OR REPLACE FUNCTION avg_diff_5_10_60(pRowID ROWID, pDiff NUMBER) RETURN INTEGER
IS
    vTrade day_trades%ROWTYPE;    
BEGIN
    
    SELECT t.* INTO vTrade FROM day_trades t WHERE t.rowid = pRowid;
    
    IF vTrade.avg60 = 0 THEN
        RETURN 0;
    END IF;
    
    IF abs(vTrade.Avg5 - vTrade.avg60) * 100 / vTrade.avg60 > pDiff THEN 
        RETURN 0;
    END IF;

    IF abs(vTrade.Avg10 - vTrade.avg60) * 100 / vTrade.avg60 > pDiff THEN 
        RETURN 0;
    END IF;
    
    RETURN (1);
END;
/
