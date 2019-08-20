SELECT
    id AS reason_id,
    description AS reason_name,
    1 AS group_reason_id,
    'online' AS group_reason_name
FROM dwh_staging.failed_reasons;