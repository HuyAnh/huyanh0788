DELETE FROM
    dwh_staging.sale_details
WHERE
    contactc3_id IN
        (
            SELECT
                contactc3_id
            FROM
                dwh_staging.v_batched_no_course_bifrost_nami_or_bifrost_contactc3_ids
        )
    AND grouped_by_course = 'false';
