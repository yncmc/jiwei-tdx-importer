CREATE OR REPLACE PROCEDURE cal_share_profit(
    pCode IN VARCHAR2, 
    pFromId INT 
)
IS
    vProfit Day_profits%ROWTYPE;
BEGIN
    DELETE day_profits t WHERE t.code = pCode;

    FOR cur IN (
        SELECT * 
        FROM day_trades t 
        WHERE t.code = pCode 
            AND t.tradeday >= DATE'2010-1-1' 
            AND t.id >= pFromId
        ORDER BY t.id)
    LOOP
        vProfit.Tradeday := cur.tradeday;
        vProfit.Code := cur.code;
        vProfit.name := cur.name;
        vProfit.id := cur.id;
        vProfit.B2 := cal_before_profit(cur.code, cur.id, 2);
        vProfit.B3 := cal_before_profit(cur.code, cur.id, 3);
        vProfit.B4 := cal_before_profit(cur.code, cur.id, 4);
        vProfit.B5 := cal_before_profit(cur.code, cur.id, 5);
        vProfit.B6 := cal_before_profit(cur.code, cur.id, 6);
        vProfit.B7 := cal_before_profit(cur.code, cur.id, 7);
        vProfit.B8 := cal_before_profit(cur.code, cur.id, 8);
        vProfit.B9 := cal_before_profit(cur.code, cur.id, 9);
        vProfit.B10 := cal_before_profit(cur.code, cur.id, 10);
        vProfit.B11 := cal_before_profit(cur.code, cur.id, 11);
        vProfit.B12 := cal_before_profit(cur.code, cur.id, 12);
        vProfit.B13 := cal_before_profit(cur.code, cur.id, 13);
        vProfit.B14 := cal_before_profit(cur.code, cur.id, 14);
        vProfit.B15 := cal_before_profit(cur.code, cur.id, 15);
        vProfit.B16 := cal_before_profit(cur.code, cur.id, 16);
        vProfit.B17 := cal_before_profit(cur.code, cur.id, 17);
        vProfit.B18 := cal_before_profit(cur.code, cur.id, 18);
        vProfit.B19 := cal_before_profit(cur.code, cur.id, 19);
        vProfit.B20 := cal_before_profit(cur.code, cur.id, 20);
        vProfit.B25 := cal_before_profit(cur.code, cur.id, 25);
        vProfit.B30 := cal_before_profit(cur.code, cur.id, 30);
        vProfit.B60 := cal_before_profit(cur.code, cur.id, 60);

        vProfit.a2 := cal_after_profit(cur.code, cur.id, 2);
        vProfit.a3 := cal_after_profit(cur.code, cur.id, 3);
        vProfit.a4 := cal_after_profit(cur.code, cur.id, 4);
        vProfit.a5 := cal_after_profit(cur.code, cur.id, 5);
        vProfit.a6 := cal_after_profit(cur.code, cur.id, 6);
        vProfit.a7 := cal_after_profit(cur.code, cur.id, 7);
        vProfit.a8 := cal_after_profit(cur.code, cur.id, 8);
        vProfit.a9 := cal_after_profit(cur.code, cur.id, 9);
        vProfit.a10 := cal_after_profit(cur.code, cur.id, 10);
        vProfit.a11 := cal_after_profit(cur.code, cur.id, 11);
        vProfit.a12 := cal_after_profit(cur.code, cur.id, 12);
        vProfit.a13 := cal_after_profit(cur.code, cur.id, 13);
        vProfit.a14 := cal_after_profit(cur.code, cur.id, 14);
        vProfit.a15 := cal_after_profit(cur.code, cur.id, 15);
        vProfit.a16 := cal_after_profit(cur.code, cur.id, 16);
        vProfit.a17 := cal_after_profit(cur.code, cur.id, 17);
        vProfit.a18 := cal_after_profit(cur.code, cur.id, 18);
        vProfit.a19 := cal_after_profit(cur.code, cur.id, 19);
        vProfit.a20 := cal_after_profit(cur.code, cur.id, 20);
        vProfit.a25 := cal_after_profit(cur.code, cur.id, 25);
        vProfit.a30 := cal_after_profit(cur.code, cur.id, 30);
        vProfit.a60 := cal_after_profit(cur.code, cur.id, 60);

        INSERT INTO day_profits VALUES vProfit;
    END LOOP;
END;
/
