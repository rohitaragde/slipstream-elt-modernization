SELECT
    Plan_type              AS plan_type,
    "Plan Description"     AS plan_description,
    Data_rate_per_kb       AS data_rate_per_kb,
    " free_voice_minutes"  AS free_voice_minutes,
    " free_sms"            AS free_sms,
    " rate_per_minute"     AS rate_per_minute,
    sms_rate,
    run_date
FROM {{ source('slipstream', 'stg_plan_master') }}