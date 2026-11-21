with bounds as (
    select
        cast(min(order_purchase_timestamp) as date) as min_date,
        cast(max(order_purchase_timestamp) as date) as max_date
    from {{ source('olist_silver', 'orders') }}
), calendar as (
    select explode(sequence(min_date, max_date, interval 1 day)) as calendar_date
    from bounds
)
select
    0 as date_key,
    cast(null as date) as calendar_date,
    0 as calendar_year,
    0 as calendar_quarter,
    0 as calendar_month,
    'Unknown' as month_name,
    0 as week_of_year,
    0 as day_of_month,
    0 as day_of_week
union all
select
    cast(date_format(calendar_date, 'yyyyMMdd') as int) as date_key,
    calendar_date,
    year(calendar_date) as calendar_year,
    quarter(calendar_date) as calendar_quarter,
    month(calendar_date) as calendar_month,
    date_format(calendar_date, 'MMMM') as month_name,
    weekofyear(calendar_date) as week_of_year,
    day(calendar_date) as day_of_month,
    dayofweek(calendar_date) as day_of_week
from calendar
