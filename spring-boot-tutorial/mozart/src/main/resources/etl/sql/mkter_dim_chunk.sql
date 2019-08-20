WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT nu.id    AS mkter_id,
       nu.name  AS mkter_name,
       nu.email AS mkter_email,
       nr.code  AS mkter_role_code,
       nr.name  AS mkter_role_name
FROM dwh_staging.nami_users nu, mozart_batch_information mbi
LEFT JOIN dwh_staging.nami_roles nr ON nu.nami_role_id = nr.id
WHERE (nu.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (nr.transformed_at BETWEEN mbi.start_time AND mbi.end_time)