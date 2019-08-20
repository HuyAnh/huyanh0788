UPDATE dwh_staging.mozart_transformation
SET info = ?,
    job_name = ?,
    updated_at = ?
WHERE name = ?;