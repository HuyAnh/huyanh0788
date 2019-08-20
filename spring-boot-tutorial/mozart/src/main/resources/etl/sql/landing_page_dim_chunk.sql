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
    hlp.id          AS landing_page_id,
    hlp.name        AS landing_page_name,
    hd.id           AS domain_id,
    hd.name         AS domain_name,
    hd.status       AS domain_status
FROM
    mozart_batch_information            AS mbi,
    dwh_staging.hera_landing_pages      AS hlp
    LEFT JOIN dwh_staging.hera_domains  AS hd ON hlp.hera_domain_id = hd.id
WHERE
    (hlp.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (hd.transformed_at BETWEEN mbi.start_time AND mbi.end_time)