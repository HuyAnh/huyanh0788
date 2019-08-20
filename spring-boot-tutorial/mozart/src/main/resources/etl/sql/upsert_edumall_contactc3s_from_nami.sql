WITH mozart_batch_information AS
  (SELECT st.info AS start_time,
          et.info AS end_time
   FROM
     (SELECT COALESCE(mt.info, '2015-01-01') AS info
      FROM dwh_staging.mozart_transformation mt
      WHERE name = 'start_time') st
   JOIN
     (SELECT info
      FROM dwh_staging.mozart_transformation
      WHERE name = 'end_time') et)
UPSERT INTO dwh_staging.edumall_contactc3s
SELECT nmcts.origin_id AS id,
       nmcts.customer_name AS name,
       nmcts.customer_mobile AS mobile,
       nmcts.customer_email AS email,
       'bifrost_transaction' AS TYPE,
       nmcts.ad_source AS SOURCE,
       nmcts.origin_url AS source_url,
       nmcts.strategy,
       nmcts.status,
       nmcts.origin_created_at AS created_at,
       CASE
           WHEN sps.code IS NULL THEN nmcs.source_id
           ELSE NULL
       END course_id,
       sps.code AS combo_code,
       ccus.nami_user_id AS mkter_id,
       gusrs.nami_group_id AS sub_department_id,
       ads.id AS ad_id,
       chcs.nami_channel_id AS channel_ad_id,
       adgs.nami_campaign_id AS campaign_id,
       cps.nami_adaccount_id AS ad_account_id,
       ads.nami_ad_group_id AS ad_group_id,
       'Bifrost_Nami' source_system,
        mbi.end_time AS transformed_at,
        NULL AS auto_cod_process,
        NULL AS auto_cod_status,
        NULL AS cod,
        NULL AS form_id,
        split_part(nmcts.origin_url, '/', 3) AS landing_page_domain
FROM dwh_staging.nami_contactc3s nmcts, mozart_batch_information mbi
LEFT JOIN dwh_staging.nami_ads ads ON ads.utm_medium = nmcts.ad_code
AND ads.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_ad_groups adgs ON adgs.id = ads.nami_ad_group_id
AND adgs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_campaigns cps ON cps.id = adgs.nami_campaign_id
AND cps.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_channel_courses chcs ON chcs.id = cps.nami_channel_course_id
AND chcs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_courses nmcs ON nmcs.id = chcs.nami_course_id
AND nmcs.deleted_at IS NULL
LEFT JOIN
  (SELECT DISTINCT code
   FROM dwh_staging.jackfruit_sale_packages) sps ON sps.code = nmcs.source_id
LEFT JOIN dwh_staging.nami_channel_course_users ccus ON ccus.nami_channel_course_id = chcs.id
AND ccus.deleted_at IS NULL
AND TO_DATE(nmcts.submitted_at) >= TO_DATE(ccus.start_date)
AND TO_DATE(nmcts.submitted_at)<= TO_DATE(ccus.end_date)
LEFT JOIN dwh_staging.nami_users usr ON usr.id = ccus.nami_user_id
AND usr.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_group_users gusrs ON gusrs.nami_user_id =usr.id
AND gusrs.deleted_at IS NULL
AND TO_DATE(nmcts.submitted_at) >= TO_DATE(gusrs.start_date)
AND TO_DATE(nmcts.submitted_at)<= TO_DATE(gusrs.end_date)
WHERE (
  (nmcts.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (ads.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (adgs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (cps.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (chcs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (ccus.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (usr.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (gusrs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (nmcs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
)
  AND nmcts.origin_from = 'bifrost_transaction'
  AND EXISTS
  (SELECT 1
    FROM dwh_staging.bifrost_transactions  ts
    WHERE cast(ts.id AS string)  = nmcts.origin_id
    AND (ts.siren_id IS NULL OR ts.siren_id = '') )
  AND COALESCE(nmcts.status, 'a') NOT IN ('duplicated')
  AND COALESCE(nmcts.strategy, 'a') NOT IN ('test');