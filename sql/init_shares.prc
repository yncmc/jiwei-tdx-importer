CREATE OR REPLACE PROCEDURE init_shares IS
BEGIN

    DELETE shares;
    
    INSERT INTO shares(code, NAME, Offeringdate)
    SELECT t.code, MAX(t.name), MIN(t.tradeday)
    FROM day_trades t
    GROUP BY t.code;
    
    --EXECUTE IMMEDIATE 'truncate table day_profits';
END;
/
