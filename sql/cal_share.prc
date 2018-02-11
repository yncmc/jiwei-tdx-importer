CREATE OR REPLACE PROCEDURE cal_share(pCode IN VARCHAR2) IS
BEGIN
    UPDATE day_trades t
    SET t.basepx = NVL((SELECT t2.closePx FROM day_trades t2 WHERE t2.code = pCode AND t2.id = t.id - 1), 0)
    WHERE t.code = pCode;
    
    cal_share_avg(pCode);
    --cal_share_profit(pCode, 1);
    cal_share_macd(pCode);
END;
/
