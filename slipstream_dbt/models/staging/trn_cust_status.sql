SELECT
    sbscrbr,
    COALESCE(
        try_strptime(Paid_Date, '%-m/%-d/%Y'),
        try_strptime(Paid_Date, '%-d/%-m/%Y')
    ) AS paid_date,
    run_date
FROM {{ source('slipstream', 'stg_cust_status') }}