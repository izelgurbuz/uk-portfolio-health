CREATE OR REPLACE VIEW PORTFOLIO.ANALYTICS.VIEW_PORTFOLIO_METRICS AS
WITH holdings AS (
    SELECT
        pos.PORTFOLIO_ID,
        fp.DATE,
        fp.SYMBOL,
        fp.CLOSE_GBP,
        fp.DAILY_RETURN,
        pos.QUANTITY,
        (fp.CLOSE_GBP * pos.QUANTITY) AS position_value_gbp
    FROM PORTFOLIO.ANALYTICS.FACT_PRICES fp
    JOIN PORTFOLIO.ANALYTICS.PORTFOLIO_POSITIONS_DAILY pos
      ON fp.SYMBOL = pos.SYMBOL
      AND fp.DATE   = pos.DATE
), -- spesific symbol for the spesific portfolio each day

--  daily portfolio returns
-- Portfolio daily return = (sum of each position’s profit or loss) / (total portfolio value)
-- We have to think about components of the calculations seperately, independently 
daily AS (
    SELECT
        PORTFOLIO_ID,
        DATE,
        SUM(position_value_gbp) AS total_value_gbp,
        SUM(DAILY_RETURN * position_value_gbp) / NULLIF(SUM(position_value_gbp), 0) AS weighted_daily_return
    FROM holdings
    GROUP BY PORTFOLIO_ID, DATE
),

-- rolling volatility of daily return
with_vol AS (
    SELECT
        d.*,
        STDDEV_SAMP(d.weighted_daily_return) OVER (
            PARTITION BY d.PORTFOLIO_ID ORDER BY d.DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30d_volatility
    FROM daily d
)

SELECT
    v.PORTFOLIO_ID,
    v.DATE,
    v.total_value_gbp,
    v.weighted_daily_return,
    v.rolling_30d_volatility,

    -- 30-day VaR for each row
    -- Snowflake does not let us use percentile continuos with window
    -- so I created a subquery calculating VaR for each group - portfolio - 
    -- calculating VaR for each row depending on the portfolio id and date values of the row
    -- 'within group' acts similar to group by but for sorted lists and taking whole set as the group
    -- thinking about logics seperately helps us here as well. Group is limited/defined by the parent's portfolio id value 
    --  and we guarantee that this value will be added to each row in original table because we are using subquery in select
    (
        SELECT PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY d2.weighted_daily_return)
        FROM daily d2
        WHERE d2.PORTFOLIO_ID = v.PORTFOLIO_ID
          AND d2.DATE BETWEEN DATEADD(DAY, -29, v.DATE) AND v.DATE
    ) AS daily_var_95

FROM with_vol v;
