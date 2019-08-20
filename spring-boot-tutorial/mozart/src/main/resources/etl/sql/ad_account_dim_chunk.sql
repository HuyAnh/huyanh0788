WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT na.id AS ad_account_id,
       na.name AS ad_account_name,
       na.status AS ad_account_status,
       np.id AS provider_id,
       np.name AS provider_name
FROM dwh_staging.nami_adaccounts na, mozart_batch_information mbi
LEFT JOIN dwh_staging.nami_providers np ON na.nami_provider_id = np.id
WHERE
    (na.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (np.transformed_at BETWEEN mbi.start_time AND mbi.end_time)