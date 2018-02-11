CREATE OR REPLACE PROCEDURE recal_init IS
BEGIN
    DELETE shares;
    COMMIT;
    
    INSERT INTO shares(code, name, offeringdate)
    SELECT t.code, MAX(t.name), MIN(t.tradeday)
    FROM day_trades t
    GROUP BY t.code;
    COMMIT;
    
    FOR cur IN (
        SELECT s.*
        FROM SHARES s 
        WHERE NOT EXISTS (SELECT 1 FROM day_trades t WHERE t.code = s.code AND t.ema12 <> 0))
    LOOP
        dbms_output.put_line(cur.code || ',' || cur.name);
        cal_share_avg(cur.code);
        cal_share_profit(cur.code, 1);
        cal_share_macd(cur.code);
    END LOOP;
END;
/
