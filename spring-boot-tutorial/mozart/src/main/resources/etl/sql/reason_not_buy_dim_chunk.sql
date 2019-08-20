WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT
    ernb.id             AS reason_not_buy_id,
    ernb.reason_not_buy AS reason_not_buy_name
FROM
    mozart_batch_information        AS mbi,
    dwh_staging.eros_reason_not_buy AS ernb
WHERE
    (ernb.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
