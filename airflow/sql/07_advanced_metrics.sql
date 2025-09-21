with base as(
    select portfolio_id, date, weighted_daily_return as daily_return
    from VIEW_PORTFOLIO_METRICS
),
stats as (
    select
        portfolio_id,
        date, 
        avg(daily_return) over (partition by portfolio_id order by date rows between 29 preceding and current row) as avg_portfolio_return_30d,
        stddev_samp(daily_return) over (partition by portfolio_id order by date rows between 29 preceding and current row) as vol_return_30d
    from base
)

downside_vol as(
    select
        portfolio_id,
        date,
        stddev_samp(daily_return) filter( where daily_return < 0) over (partition by portfolio_id order by date rows between 29 preceding and current row) as down_vol_return_30d
    from base

)

select 
    b.portfolio_id, 
    b.date, 
    case 
        when s.vol_return_30d > 0 then s.avg_portfolio_return_30d / s.vol_return_30d 
        else 0 
    end as sharpe,
    case 
        when d.down_vol_return_30d > 0 then d.avg_portfolio_return_30d / d.down_vol_return_30d 
        else 0 
    end as sortino
from base b
left join stats s
on b.portfolio_id = s.portfolio_id
and b.date = s.date
left join downside_vol d
on b.portfolio_id = d.portfolio_id
and b.date = d.date
