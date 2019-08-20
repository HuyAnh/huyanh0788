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
    na.id               AS ad_id,
    na.name             AS ad_name,
    na.utm_source       AS utm_source,
    na.utm_medium       AS utm_medium,
    na.utm_campaign     AS utm_campaign,
    na.tracking_link    AS tracking_link,
    na.status           AS status,
    nag.id              AS ad_group_id,
    nag.name            AS ad_group_name
FROM
    mozart_batch_information    AS mbi,
    dwh_staging.nami_ads        AS na
    LEFT JOIN dwh_staging.nami_ad_groups   AS nag ON na.nami_ad_group_id = nag.id
WHERE
    (na.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (nag.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
