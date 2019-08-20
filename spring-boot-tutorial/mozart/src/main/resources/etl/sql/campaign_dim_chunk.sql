WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT nc.id AS campaign_id,
       nc.name AS campaign_name,
       nc.status AS campaign_status,
       CASE
           WHEN nc.status = 1 THEN 'ENABLED'
           WHEN nc.status = 2 THEN 'PAUSED'
           WHEN nc.status = 3 THEN 'DISABLED'
           WHEN nc.status = 5 THEN 'REMOVED'
           ELSE NULL
       END AS campaign_status_name
FROM dwh_staging.nami_campaigns nc, mozart_batch_information mbi
WHERE nc.transformed_at BETWEEN mbi.start_time AND mbi.end_time