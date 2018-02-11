CREATE OR REPLACE PROCEDURE cal_daily_avg(pDate IN DATE DEFAULT TRUNC(SYSDATE))
IS
    vTrade day_trades%ROWTYPE;
BEGIN
   FOR cur IN (SELECT * FROM day_trades t WHERE t.tradeday = pDate)
   LOOP
       vTrade.Avg5 := cal_avg(cur.code, cur.id, 5);
       --dbms_output.put_line(cur.id || ',' || cur.code || ',' || vTrade.Avg5);
       vTrade.Avg10 := cal_avg(cur.code, cur.id, 10);
       vTrade.Avg20 := cal_avg(cur.code, cur.id, 20);
       vTrade.Avg25 := cal_avg(cur.code, cur.id, 25);
       vTrade.Avg30 := cal_avg(cur.code, cur.id, 30);
       vTrade.Avg60 := cal_avg(cur.code, cur.id, 60);
       
        UPDATE day_trades t
        SET t.avg5 = vTrade.Avg5,
            t.avg10 = vTrade.Avg10,
            t.avg20 = vTrade.Avg20,
            t.avg25 = vTrade.Avg25,
            t.avg30 = vTrade.Avg30,
            t.avg60 = vTrade.Avg60
        WHERE t.code = cur.code 
            AND t.id = cur.id;       
   END LOOP;
END;
/
