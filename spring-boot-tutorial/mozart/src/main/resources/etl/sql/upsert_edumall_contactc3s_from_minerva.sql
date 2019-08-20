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
SELECT cts.id,
       cts.name,
       cts.mobile,
       cts.email,
       cts.type,
       cts.source,
       cts.source_url,
       cts.strategy,
       cts.status,
       cts.created_at,
       CASE
           WHEN sps.code IS NULL THEN cts.jackfruit_course_id
           ELSE NULL
       END course_id,
       sps.code AS combo_code,
       ccus.nami_user_id mkter_id,
       CASE
           WHEN cts.type = 'c3_tele' THEN 1000
           WHEN cts.source IN ('edumall-aff',
                               'edumall-mkt',
                               'edumall-eCommerce',
                               'edumall',
                               'pidi',
                               'edumall-eCommerce-mkt') THEN 2001
           WHEN cts.source IN ('app_mobile_android',
                               'app_mobile',
                               'app_mobile_ios') THEN 2002
           ELSE gusrs.nami_group_id
       END sub_department_id,
       ads.id AS ad_id,
       chcs.nami_channel_id AS channel_ad_id,
       adgs.nami_campaign_id AS campaign_id,
       cps.nami_adaccount_id AS ad_account_id,
       ads.nami_ad_group_id AS ad_group_id,
       'Minerva' source_system,
        mbi.end_time AS transformed_at,
        cts.auto_cod_process AS auto_cod_process,
        cts.auto_cod_status AS auto_cod_status,
        cts.cod AS cod,
        cts.form_id AS form_id,
        split_part(cts.source_url, '/', 3) AS landing_page_domain
FROM dwh_staging.minerva_contactc3s cts, mozart_batch_information mbi
LEFT JOIN
  (SELECT DISTINCT code
   FROM dwh_staging.jackfruit_sale_packages)sps ON sps.code = cts.jackfruit_course_id
LEFT JOIN dwh_staging.nami_contactc3s nmcts ON nmcts.origin_id = cts.id
LEFT JOIN dwh_staging.nami_ads ads ON ads.utm_medium = nmcts.ad_code
AND ads.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_ad_groups adgs ON adgs.id = ads.nami_ad_group_id
AND adgs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_campaigns cps ON cps.id = adgs.nami_campaign_id
AND cps.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_channel_courses chcs ON chcs.id = cps.nami_channel_course_id
AND chcs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_channel_course_users ccus ON ccus.nami_channel_course_id = chcs.id
AND ccus.deleted_at IS NULL
AND TO_DATE(cts.created_at) >= TO_DATE(ccus.start_date)
AND TO_DATE(cts.created_at)<= TO_DATE(ccus.end_date)
LEFT JOIN dwh_staging.nami_users usr ON usr.id = ccus.nami_user_id
AND usr.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_group_users gusrs ON gusrs.nami_user_id =usr.id
AND gusrs.deleted_at IS NULL
AND TO_DATE(cts.created_at) >= TO_DATE(gusrs.start_date)
AND TO_DATE(cts.created_at)<= TO_DATE(gusrs.end_date)
WHERE (
  (cts.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (nmcts.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (ads.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (adgs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (cps.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (chcs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (ccus.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (usr.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (gusrs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
)
  AND COALESCE(cts.status, 'a') NOT IN ('duplicated')
  AND COALESCE (cts.strategy,
                'a') NOT IN ('test') ;