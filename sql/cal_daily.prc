CREATE OR REPLACE PROCEDURE cal_daily
IS
    vDate DATE := TRUNC(SYSDATE);
BEGIN
    recal_init;
    
    cal_daily_avg(vDate);
    cal_daily_profit(vDate);
    cal_daily_macd(vdate);
    --init_macd();
    
    DELETE shares;
    
    INSERT INTO shares(code, name, offeringdate)
    SELECT t.code, MIN(t.name), MIN(t.tradeday)
    FROM day_trades t
    GROUP BY t.code;
END;
/
