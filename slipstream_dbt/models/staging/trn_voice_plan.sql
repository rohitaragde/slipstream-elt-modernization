SELECT
    vp.sbscrbr,
    vp.Plan_Type AS plan_type,

    vp.voice_time,
    ROUND(LEAST(vp.voice_time, pm.free_voice_minutes) * pm.rate_per_minute, 2)
        AS non_billable_voice_amt,
    CASE
        WHEN vp.voice_time > pm.free_voice_minutes
        THEN ROUND((vp.voice_time - pm.free_voice_minutes) * pm.rate_per_minute, 2)
        ELSE 0
    END AS billable_voice_amt,

    vp.sms_used,
    ROUND(LEAST(vp.sms_used, COALESCE(pm.free_sms, vp.sms_used)) * pm.sms_rate, 2)
        AS non_billable_sms_amt,
    CASE
        WHEN pm.free_sms IS NULL THEN 0
        WHEN vp.sms_used > pm.free_sms
        THEN ROUND((vp.sms_used - pm.free_sms) * pm.sms_rate, 2)
        ELSE 0
    END AS billable_sms_amt,

    COALESCE(
        try_strptime(vp.load_date, '%-m/%-d/%Y'),
        try_strptime(vp.load_date, '%-d/%-m/%Y')
    ) AS load_date,
    vp.run_date
FROM stg_voice_plan vp
LEFT JOIN {{ ref('trn_plan_master') }} pm
    ON vp.Plan_Type = pm.plan_type
