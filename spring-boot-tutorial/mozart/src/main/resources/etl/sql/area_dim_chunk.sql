WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT gmd.id AS district_id,
       gmd.code AS district_code,
       gmd.name AS district_name,
       gmp.code AS province_code,
       gmp.name AS provine_name,
       gmp.region AS region_name,
       'VN' AS country_code,
       'Việt Nam' AS country_name
FROM dwh_staging.gambit_map_districts gmd, mozart_batch_information mbi
LEFT JOIN dwh_staging.gambit_map_provinces gmp ON gmd.gambit_province_id = gmp.id
WHERE
    (gmd.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (gmp.transformed_at BETWEEN mbi.start_time AND mbi.end_time)