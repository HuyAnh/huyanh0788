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
    gp.id           AS partner_id,
    gp.code         AS partner_code,
    gp.name         AS partner_name,
    gp.status       AS partner_status,
    'COD'           AS partner_type
FROM
    mozart_batch_information    AS mbi,
    dwh_staging.gambit_partners AS gp
WHERE
    (gp.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
