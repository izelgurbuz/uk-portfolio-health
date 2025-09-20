MERGE INTO PORTFOLIO.ANALYTICS.PORTFOLIO_POSITIONS_DAILY t
USING (
    WITH params AS (
        select
            COALESCE(
                (select last_loaded_date
            from PORTFOLIO.RAW.LOAD_METADATA
            where source = 'positions_daily'),
                DATEADD(day,-1, (select min(transaction_date) from PORTFOLIO.RAW.PORTFOLIO_TRANSACTIONS ))
            ) as last_date
    ),
    base AS(
        select distinct symbol, portfolio_id
        from PORTFOLIO.RAW.PORTFOLIO_TRANSACTIONS
    ),
    priors AS(
        select b.symbol, b.portfolio_id, COALESCE(d.quantity, 0) as prior_qty
        from base b
        left join PORTFOLIO.ANALYTICS.PORTFOLIO_POSITIONS_DAILY d
        on b.symbol = d.symbol 
        and b.portfolio_id = d.portfolio_id
        and d.date = (select last_date from params)
    ),
    deltas AS(
        select t.portfolio_id, t.symbol, t.transaction_date as date,SUM( quantity_delta) as qty_delta
        from PORTFOLIO.RAW.PORTFOLIO_TRANSACTIONS t
        where t.transaction_date > (select last_date from params)
        and t.transaction_date <= CURRENT_DATE()
    group by 1,2,3
    ),

    date_spine AS (
    SELECT DATEADD(day, seq4(), DATEADD(day, 1, (SELECT last_date FROM params))) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))  -- pick a safe upper bound
    WHERE date <= CURRENT_DATE
    ),

    matrix AS(
        select b.portfolio_id,b.symbol, d.date, COALESCE(dl.qty_delta,0) as qty_delta
        from base b
        cross join date_spine d
        left join deltas dl
        on b.symbol = dl.symbol
        and b.portfolio_id = dl.portfolio_id
        and dl.date = d.date
    ),
    calc AS(
        select 
            m.symbol, m.portfolio_id, m.date , p.prior_qty + SUM(m.qty_delta) over(
                partition by m.symbol, m.portfolio_id 
                order by m.date
                ROWS UNBOUNDED PRECEDING
                ) as quantity
        from matrix m 
        left join priors p
        on p.portfolio_id = m.portfolio_id
        and p.symbol = m.symbol
    )
    SELECT * FROM calc
) c
ON t.portfolio_id = c.portfolio_id
AND t.symbol = c.symbol
AND t.date = c.date
WHEN MATCHED THEN UPDATE SET quantity = c.quantity
WHEN NOT MATCHED THEN INSERT (portfolio_id, symbol, date, quantity)
VALUES (c.portfolio_id, c.symbol, c.date, c.quantity);

merge into PORTFOLIO.RAW.LOAD_METADATA target
using (select 'positions_daily' AS source, CURRENT_DATE AS last_loaded_date) src
on target.source = src.source
when matched then update set last_loaded_date = src.last_loaded_date, _updated_at = CURRENT_TIMESTAMP()
when not matched then insert (source, last_loaded_date) values (src.source, src.last_loaded_date);