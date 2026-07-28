SELECT
    sbscrbr,
    REPLACE(user_id, '_', ' ') AS customer_name,
    CASE loyalty_badge
        WHEN 'BRNZ' THEN 'Bronze'
        WHEN 'SLVR' THEN 'Silver'
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'PLAT' THEN 'Platinum'
    END AS loyalty_badge,
    loyalty_spent,
    loyalty_accrue,
    COALESCE(
        try_strptime(REPLACE(Reg_date_time, '-', '/'), '%-m/%-d/%Y'),
        try_strptime(REPLACE(Reg_date_time, '-', '/'), '%-d/%-m/%Y')
    ) AS reg_date,
    run_date
FROM stg_loyalty