CREATE OR REPLACE PROCEDURE init_profit IS
BEGIN
    FOR cur IN (
        SELECT * FROM shares s 
        WHERE NOT EXISTS (SELECT 1 FROM day_profits t WHERE t.code = s.code)
        ORDER BY s.code)
    LOOP
        dbms_output.put_line(cur.code);
        cal_share_profit(cur.code, 0);
        COMMIT;
    END LOOP;
END;
/
