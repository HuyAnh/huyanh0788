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
    nc.id           AS channel_ad_id,
    nc.code         AS channel_ad_code,
    nc.name         AS channel_ad_name
FROM
    mozart_batch_information    AS mbi,
    dwh_staging.nami_channels   AS nc
WHERE
    (nc.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
