WITH ordered_menu AS(
    SELECT
        o.order_id,
        o.menu_id,
        o.quantity,
        o.sales_date,
        m.brand,
        m.name,
        m.price price_per_menu,
        m.cogs cogs_per_menu,
        m.price * o.quantity total_price,
        m.cogs * o.quantity total_cogs,
        m.effective_date,
        ROW_NUMBER() OVER (PARTITION BY
            o.order_id, o.menu_id
            ORDER BY m.effective_date DESC) as rn
    FROM
        staging.Order_Staging o
    LEFT JOIN
        staging.Menu_Staging m
    ON
        o.menu_id = m.menu_id AND
        o.sales_date >= m.effective_date
),
ordered_menu_promo AS(
    SELECT 
        om.order_id,
        om.menu_id,
        om.quantity,
        om.sales_date,
        om.brand,
        om.name,
        om.price_per_menu AS price_per_menu_befdisc,
        COALESCE(
            CASE
                WHEN p.start_date <= om.sales_date AND om.sales_date <= p.end_date THEN 
                    om.price_per_menu - LEAST(p.disc_value*om.price_per_menu, p.max_disc)
            END, 
            om.price_per_menu
        ) AS price_per_menu_afdisc,
        COALESCE(
            CASE
                WHEN p.start_date <= om.sales_date AND om.sales_date <= p.end_date THEN 
                    LEAST(p.disc_value*om.price_per_menu, p.max_disc)
            END, 
            0
        ) AS disc_value,
        om.cogs_per_menu,
        total_cogs,
        om.total_price AS total_price_befdisc
    FROM 
        ordered_menu om
    LEFT JOIN 
        staging.Promotion_Staging p
    ON 
        om.sales_date BETWEEN p.start_date AND p.end_date
    WHERE 
        om.rn = 1
    ORDER BY 
        om.sales_date
)
SELECT
    *,
    price_per_menu_afdisc * quantity AS total_price_afdisc
FROM
    ordered_menu_promo