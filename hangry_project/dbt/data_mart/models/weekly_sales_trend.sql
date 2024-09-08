select 
    week(sales_date,7) AS week,

    sum(total_price_befdisc) as gross_revenue_weekly,
    sum(quantity) as total_quantity,
    sum(total_cogs) as total_cogs_weekly,
    sum(total_price_befdisc)-sum(total_cogs) as total_profit_befdisc,
    sum(total_price_befdisc) / sum(quantity) as avg_price_per_item_befdisc,
    ((sum(total_price_befdisc)-sum(total_cogs)) / sum(total_price_befdisc)) * 100 as avg_profit_margin_befdisc,

    sum(disc_value) as total_discount,

    sum(total_price_afdisc) as total_price_afdisc,
    sum(total_price_afdisc)-sum(total_cogs) as net_profit_weekly,
    sum(total_price_afdisc) / sum(quantity) as avg_price_per_item_afdisc,
    ((sum(total_price_afdisc)-sum(total_cogs)) / sum(total_price_afdisc)) * 100 as avg_profit_margin_afdisc

from staging.daily_sales_transaction
group by week(sales_date,7)
order by week(sales_date,7)