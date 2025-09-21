merge into PORTFOLIO.ANALYTICS.FACT_PORTFOLIO_ADV_METRICS t
using(
    with base as(
        select portfolio_id, date, weighted_daily_return as daily_return, total_value_gbp
        from PORTFOLIO.ANALYTICS.VIEW_PORTFOLIO_METRICS
    ),
    stats_portfolio as (
        select
            portfolio_id,
            date, 
            avg(daily_return) over (partition by portfolio_id order by date rows between 29 preceding and current row) as avg_portfolio_return_30d,
            stddev_samp(daily_return) over (partition by portfolio_id order by date rows between 29 preceding and current row) as vol_return_30d
        from base
    ),

    downside_vol as(
        select
            portfolio_id,
            date,
            stddev_samp(IFF(daily_return < 0, daily_return, NULL)) over (partition by portfolio_id order by date rows between 29 preceding and current row) as down_vol_return_30d
        from base

    ),
    running_peaks as (
        select 
            portfolio_id,
            date,
            total_value_gbp,
            max(total_value_gbp) over (partition by portfolio_id order by date ) as running_peak
        from base
    ),
    drawdowns as (
        select 
            portfolio_id,
            date,
            case 
                when running_peak > 0 then (total_value_gbp - running_peak) / running_peak
                else 0
            end as drawdown
        from running_peaks
    ),

    benchmark_daily as (
        select 
            date,
            symbol,
            ((close / lag(close) over (partition by symbol order by date))-1) as benchmark_return
            from PORTFOLIO.RAW.FACT_BENCHMARK
    ),
    joined as (
            select
                p.DATE,
                p.PORTFOLIO_ID,
                p.daily_return as portfolio_return,
                b.benchmark_return
            from base p
            join benchmark_daily b on p.DATE = b.DATE
    ),
    -- Rp : daily return portfolio, Rb: daily return benchmark

    --Covariance(Rp, Rb) = ( SUM(Rp * Rb) - ( SUM(Rp) * SUM(Rb) ) / n ) / (n - 1)

    --Variance(Rb) = ( SUM(Rb^2) - ( SUM(Rb) * SUM(Rb) ) / n ) / (n - 1)

    --Beta = Covariance(Rp, Rb) / Variance(Rb)

    --Alpha = Avg(Rp) - Beta * Avg(Rb)
    -- window : (
        --     partition by portfolio_id
        --     order by date
        --     rows between 29 preceding and current row
        -- )
    stats_ba as (
        select
            date,
            portfolio_id,

            -- rolling covariance
            case when count(*) over (
                partition by portfolio_id 
                order by date 
                rows between 29 preceding and current row
            ) > 1 then
                (
                    sum(portfolio_return * benchmark_return) over (
                        partition by portfolio_id 
                        order by date 
                        rows between 29 preceding and current row
                    )
                    - (
                        sum(portfolio_return) over (
                            partition by portfolio_id 
                            order by date 
                            rows between 29 preceding and current row
                        ) *
                        sum(benchmark_return) over (
                            partition by portfolio_id 
                            order by date 
                            rows between 29 preceding and current row
                        )
                    ) / count(*) over (
                        partition by portfolio_id 
                        order by date 
                        rows between 29 preceding and current row
                    )
                ) / (count(*) over (
                    partition by portfolio_id 
                    order by date 
                    rows between 29 preceding and current row
                ) - 1)
            end as covariance_30d,

            -- rolling variance of benchmark
            case when count(*) over (
                partition by portfolio_id 
                order by date 
                rows between 29 preceding and current row
            ) > 1 then
                (
                    sum(benchmark_return * benchmark_return) over (
                        partition by portfolio_id 
                        order by date 
                        rows between 29 preceding and current row
                    )
                    - (
                        sum(benchmark_return) over (
                            partition by portfolio_id 
                            order by date 
                            rows between 29 preceding and current row
                        ) *
                        sum(benchmark_return) over (
                            partition by portfolio_id 
                            order by date 
                            rows between 29 preceding and current row
                        )
                    ) / count(*) over (
                        partition by portfolio_id 
                        order by date 
                        rows between 29 preceding and current row
                    )
                ) / (count(*) over (
                    partition by portfolio_id 
                    order by date 
                    rows between 29 preceding and current row
                ) - 1)
            end as variance_30d,

            -- rolling average portfolio return
            avg(portfolio_return) over (
                partition by portfolio_id 
                order by date 
                rows between 29 preceding and current row
            ) as avg_portfolio_return_30d,

            -- rolling average benchmark return
            avg(benchmark_return) over (
                partition by portfolio_id 
                order by date 
                rows between 29 preceding and current row
            ) as avg_benchmark_return_30d

        from joined
    ),
    beta_alpha as (
            select
                date,
                portfolio_id,
                case 
                    when variance_30d > 0 then covariance_30d / variance_30d
                    else null 
                end as beta,
                case 
                    when variance_30d > 0 then avg_portfolio_return_30d - (covariance_30d / variance_30d) * avg_benchmark_return_30d
                    else null
                end as alpha
            from stats_ba
    ),

        -- combined all metrics into a single final dataset
    combined as (
        select 
            b.portfolio_id, 
            b.date, 
            case 
                when s.vol_return_30d > 0 then s.avg_portfolio_return_30d / s.vol_return_30d 
                else 0 
            end as sharpe_ratio,
            case 
                when d.down_vol_return_30d > 0 then s.avg_portfolio_return_30d / d.down_vol_return_30d 
                else 0 
            end as sortino_ratio,
            min(dd.drawdown) over (partition by dd.portfolio_id order by dd.date rows between 29 preceding and current row) as max_drawdown,
            ba.beta,
            ba.alpha
        from base b
        left join stats_portfolio s
        on b.portfolio_id = s.portfolio_id
        and b.date = s.date
        left join downside_vol d
        on b.portfolio_id = d.portfolio_id
        and b.date = d.date
        left join drawdowns dd
        on b.portfolio_id = dd.portfolio_id
        and b.date = dd.date
        left join beta_alpha ba
        on b.portfolio_id = ba.portfolio_id
        and b.date = ba.date
    )
    select * from combined
) c
on t.portfolio_id = c.portfolio_id AND t.date = c.date
WHEN MATCHED THEN UPDATE SET
    sharpe_ratio = c.sharpe_ratio,
    sortino_ratio = c.sortino_ratio,
    max_drawdown = c.max_drawdown,
    beta = c.beta,
    alpha = c.alpha
WHEN NOT MATCHED THEN INSERT (
    date, portfolio_id, sharpe_ratio, sortino_ratio, max_drawdown, beta, alpha
)
VALUES (
    c.date, c.portfolio_id, c.sharpe_ratio, c.sortino_ratio, c.max_drawdown, c.beta, c.alpha
);