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
    JOIN PORTFOLIO.RAW.PORTFOLIO_POSITIONS pos
      ON fp.SYMBOL = pos.SYMBOL
),

--  daily portfolio returns
daily AS (
    SELECT
        PORTFOLIO_ID,
        DATE,
        SUM(position_value_gbp) AS total_value_gbp,
        SUM(DAILY_RETURN * position_value_gbp) / NULLIF(SUM(position_value_gbp), 0) AS weighted_daily_return
    FROM holdings
    GROUP BY PORTFOLIO_ID, DATE
),

-- rolling volatility 
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
    (
        SELECT PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY d2.weighted_daily_return)
        FROM daily d2
        WHERE d2.PORTFOLIO_ID = v.PORTFOLIO_ID
          AND d2.DATE BETWEEN DATEADD(DAY, -29, v.DATE) AND v.DATE
    ) AS daily_var_95

FROM with_vol v;
