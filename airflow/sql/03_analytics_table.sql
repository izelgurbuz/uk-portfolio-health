CREATE TABLE IF NOT EXISTS PORTFOLIO.ANALYTICS.DIM_SYMBOL (
  SYMBOL STRING PRIMARY KEY,
  COMPANY_NAME STRING,
  CURRENCY STRING
);

MERGE INTO PORTFOLIO.ANALYTICS.FACT_PRICES AS target
USING (
    SELECT
        e.SYMBOL,
        e.DATE,
        s.COMPANY_NAME,
        s.CURRENCY,
        e.OPEN,
        e.HIGH,
        e.LOW,
        e.CLOSE,
        e.VOLUME,
        fx.RATE AS USD_TO_GBP,
        e.CLOSE * fx.RATE AS CLOSE_GBP,
        ((e.CLOSE  / LAG(e.CLOSE) OVER (
            PARTITION BY e.SYMBOL ORDER BY e.DATE
        )) - 1) AS DAILY_RETURN,
        AVG(e.CLOSE) OVER (
            PARTITION BY e.SYMBOL ORDER BY e.DATE 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ROLLING_7D_AVG_CLOSE,
        STDDEV_SAMP(e.CLOSE) OVER (
            PARTITION BY e.SYMBOL ORDER BY e.DATE 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ROLLING_30D_VOLATILITY
    FROM PORTFOLIO.RAW.EQUITY_DAILY e
    JOIN PORTFOLIO.RAW.FX_DAILY fx 
      ON fx.DATE = e.DATE 
     AND fx.PAIR = 'USDGBP'
    LEFT JOIN PORTFOLIO.ANALYTICS.DIM_SYMBOL s 
      ON e.SYMBOL = s.SYMBOL
    WHERE e.DATE > COALESCE((
        SELECT last_loaded_date
        FROM PORTFOLIO.RAW.LOAD_METADATA
        WHERE source = 'fact_prices')
    , '2025-01-01'::DATE)
) AS source
ON target.SYMBOL = source.SYMBOL 
   AND target.DATE = source.DATE
WHEN MATCHED THEN UPDATE SET
    company_name = source.COMPANY_NAME,
    currency = source.CURRENCY,
    open = source.OPEN,
    high = source.HIGH,
    low = source.LOW,
    close = source.CLOSE,
    volume = source.VOLUME,
    usd_to_gbp = source.USD_TO_GBP,
    close_gbp = source.CLOSE_GBP,
    daily_return = source.DAILY_RETURN,
    rolling_7d_avg_close = source.ROLLING_7D_AVG_CLOSE,
    rolling_30d_volatility = source.ROLLING_30D_VOLATILITY
WHEN NOT MATCHED THEN
    INSERT (
        symbol, date, company_name, currency,
        open, high, low, close, volume,
        usd_to_gbp, close_gbp, daily_return,
        rolling_7d_avg_close, rolling_30d_volatility
    )
    VALUES (
        source.symbol, source.date, source.company_name, source.currency,
        source.open, source.high, source.low, source.close, source.volume,
        source.usd_to_gbp, source.close_gbp, source.daily_return,
        source.rolling_7d_avg_close, source.rolling_30d_volatility
    );


MERGE INTO PORTFOLIO.RAW.LOAD_METADATA t
USING (SELECT 'fact_prices' AS source, CURRENT_DATE AS last_loaded_date) s
ON t.source = s.source
WHEN MATCHED THEN UPDATE SET last_loaded_date = s.last_loaded_date, _updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (source, last_loaded_date) VALUES (s.source, s.last_loaded_date);