SELECT
    dp.sbscrbr,
    dp.Plan_Type AS plan_type,
    dp.data_consumed,
    ROUND(dp.data_consumed * pm.data_rate_per_kb, 2) AS billable_data_amt,
    COALESCE(
        try_strptime(REPLACE(dp.load_date, '-', '/'), '%-m/%-d/%Y'),
        try_strptime(REPLACE(dp.load_date, '-', '/'), '%-d/%-m/%Y')
    ) AS load_date,
    dp.run_date
FROM stg_data_plan dp
LEFT JOIN {{ ref('trn_plan_master') }} pm
    ON dp.Plan_Type = pm.plan_type
