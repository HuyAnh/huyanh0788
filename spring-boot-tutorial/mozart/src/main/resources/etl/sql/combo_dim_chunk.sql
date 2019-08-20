WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT id AS combo_id,
       title AS combo_name,
       code AS combo_code,
       price,
       start_date,
       end_date
FROM dwh_staging.jackfruit_sale_packages jsp, mozart_batch_information mbi
WHERE jsp.transformed_at BETWEEN mbi.start_time AND mbi.end_time