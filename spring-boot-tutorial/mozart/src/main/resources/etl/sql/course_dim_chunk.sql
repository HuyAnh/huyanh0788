WITH mozart_batch_information AS (
    SELECT
        st.info AS start_time,
        et.info AS end_time
    FROM
        (SELECT COALESCE(mt.info, '2015-01-01') AS info FROM dwh_staging.mozart_transformation mt WHERE name = 'start_time') st
        JOIN
        (SELECT info FROM dwh_staging.mozart_transformation WHERE name = 'end_time') et
)
SELECT jc.id AS course_id,
       jc.name AS course_name,
       jc.version AS course_version,
       ju.id AS teacher_id,
       ju.username AS teacher_username,
       ju.email AS teacher_email,
       ju.name AS teacher_name,
       jscc.jackfruit_sub_category_id AS sub_category_id,
       jsc.name AS sub_category_name,
       jsc.enabled AS sub_category_enabled,
       jpc.id AS category_id,
       jpc.name AS category_name,
       jpc.enabled AS category_enabled
FROM dwh_staging.jackfruit_courses jc, mozart_batch_information mbi
LEFT JOIN dwh_staging.jackfruit_users ju ON jc.jackfruit_user_id = ju.id
LEFT JOIN dwh_staging.jackfruit_sub_category_courses jscc ON jc.id = jscc.jackfruit_course_id
LEFT JOIN dwh_staging.jackfruit_sub_categories jsc ON jsc.id = jscc.jackfruit_sub_category_id
LEFT JOIN dwh_staging.jackfruit_primary_categories jpc ON jpc.id = jsc.jackfruit_primary_category_id
WHERE
    (jc.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (ju.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (jscc.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (jsc.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR
    (jpc.transformed_at BETWEEN mbi.start_time AND mbi.end_time)