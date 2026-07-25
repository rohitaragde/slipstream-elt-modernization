SELECT
    sbscrbr,
    REPLACE(user_id, '_', ' ') AS customer_name,
    strptime("D.O.B.", '%-m/%-d/%Y') AS dob,
    Gender AS gender,
    Status AS marital_status,
    SPLIT_PART(Address, ',', 1) AS address,
    CASE
        WHEN Address LIKE '%,%'
        THEN LEFT(TRIM(SPLIT_PART(Address, ',', 2)), 2)
        ELSE NULL
    END AS state_cd,
    CASE
        WHEN Address LIKE '%,%'
        THEN RIGHT(Address, 5)
        ELSE NULL
    END AS pin,
    run_date
FROM stg_cust_profile