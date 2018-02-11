CREATE OR REPLACE PROCEDURE cal_daily_profit(pDate IN DATE DEFAULT TRUNC(SYSDATE))
IS
BEGIN
    DELETE day_profits t WHERE t.tradeday = pDate;

    FOR cur IN (
        SELECT * 
        FROM day_trades t 
        WHERE t.Tradeday = pDate)
    LOOP
        cal_share_profit(cur.code, cur.id - 60 + 1);
        COMMIT;
    END LOOP;
END;
/
