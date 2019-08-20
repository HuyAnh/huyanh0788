SELECT
    COALESCE(gorders._id, morders._id) as level_id,
    morders.level AS level_product,
    gorders.level AS level_cod,
    substr(morders.level,1,2) AS level_product_parent,
    gorders.tracking_level AS level_tracking_cod
FROM edumall_lake.morders
LEFT JOIN edumall_lake.gorders
ON gorders.siren_id = morders._id
WHERE morders.flag_deleted IS NULL OR flag_deleted <> TRUE