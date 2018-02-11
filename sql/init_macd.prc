CREATE OR REPLACE PROCEDURE init_macd IS
    vEma12 NUMBER;
    vEma26 NUMBER;
    vDif NUMBER;
    vDea NUMBER;
BEGIN
    FOR cur2 IN (SELECT DISTINCT code FROM day_trades)
    LOOP        
        FOR cur IN (SELECT * FROM day_trades t WHERE t.code = cur2.code ORDER BY t.id)
        LOOP
            vEma12 := ema12(cur.code, cur.id);
            vEma26 := ema26(cur.code, cur.id);
            vDif := vEma12 - vEma26;
            vDea := dea9(cur.code, cur.id, vDif);
            --dbms_output.put_line('dea: ' || vDea);
            
            UPDATE day_trades t
            SET t.ema12 = vEma12,
                t.ema26 = vEma26,
                t.dif   = vDif,
                t.dea   = vDea,
                t.macd = (vDif - vDea) * 2
            WHERE t.code = cur.code AND t.id = cur.id;
        END LOOP;
        COMMIT;
    END LOOP;
END;
/
