WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT DISTINCT
    es.`role` AS tvts_role_code,
    es.`role` AS tvts_role_name
FROM dwh_staging.eros_staffs es, mozart_batch_information mbi
WHERE es.transformed_at BETWEEN mbi.start_time AND mbi.end_time