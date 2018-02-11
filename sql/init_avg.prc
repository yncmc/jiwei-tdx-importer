CREATE OR REPLACE PROCEDURE init_avg IS
    vTrade Day_Trades%ROWTYPE;
BEGIN
    FOR curS IN (SELECT * FROM shares)
    LOOP
        FOR cur IN (SELECT * FROM day_trades t WHERE t.code = curs.Code AND t.id >= 5 ORDER BY id)
        LOOP
            vTrade.Avg5 := cal_avg(cur.code, cur.id, 5);
            
            vTrade.Avg10 := 0;
            IF cur.id >= 10 THEN
                vTrade.Avg10 := cal_avg(cur.code, cur.id, 10);
            END IF;

            vTrade.Avg20 := 0;
            IF cur.id >= 20 THEN
                vTrade.Avg20 := cal_avg(cur.code, cur.id, 20);
            END IF;

            vTrade.Avg25 := 0;
            IF cur.id >= 25 THEN
                vTrade.Avg25 := cal_avg(cur.code, cur.id, 25);
            END IF;

            vTrade.Avg30 := 0;
            IF cur.id >= 30 THEN
                vTrade.Avg30 := cal_avg(cur.code, cur.id, 30);
            END IF;

            vTrade.Avg60 := 0;
            IF cur.id >= 60 THEN
                vTrade.Avg60 := cal_avg(cur.code, cur.id, 60);
            END IF;
            
            UPDATE day_trades t
            SET t.avg5 = vTrade.Avg5,
                t.avg10 = vTrade.Avg10,
                t.avg20 = vTrade.Avg20,
                t.avg25 = vTrade.Avg25,
                t.avg30 = vTrade.Avg30,
                t.avg60 = vTrade.Avg60
            WHERE t.code = cur.code AND t.id = cur.id;
            
            COMMIT;
        END LOOP;
    END LOOP;
END;
/
