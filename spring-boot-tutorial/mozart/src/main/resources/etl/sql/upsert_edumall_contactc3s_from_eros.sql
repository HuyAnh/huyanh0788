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
SELECT
	eo.id,
	eo.name,
	eo.mobile,
	eo.email,
	'eros_orders' `type`,
	eo.`source`,
	eo.source_url,
	eo.strategy,
	eo.status,
	eo.created_at,
	CASE
		WHEN eo.target = 'course' THEN eo.jackfruit_course_id
		ELSE NULL
	END course_id,
	CASE
		WHEN eo.target = 'combo' THEN eo.jackfruit_course_id
		ELSE NULL
	END combo_code,
	ccus.nami_user_id mkter_id,
	gusrs.nami_group_id sub_department_id,
	ad.id AS ad_id,
	chcs.nami_channel_id AS channel_ad_id,
	adgs.nami_campaign_id AS campaign_id,
	cps.nami_adaccount_id AS ad_account_id,
	ad.nami_ad_group_id AS ad_group_id,
	'Eros' AS source_system,
	mbi.end_time AS transformed_at,
	NULL AS auto_cod_process,
	NULL AS auto_cod_status,
	NULL AS cod,
	NULL AS form_id,
	split_part(eo.source_url, '/', 3) AS landing_page_domain
FROM
	(
		SELECT ord.*
	FROM
		dwh_staging.eros_orders ord
	WHERE
		NOT EXISTS (
			SELECT 1
		FROM
			dwh_staging.minerva_contactc3s cts
		WHERE
			cts.id = ord.id)
		AND NOT EXISTS (
			SELECT 1
		FROM
			dwh_staging.kitty_order_courses kod
		WHERE
			kod.eros_order_id = ord.id) ) eo, mozart_batch_information mbi
LEFT JOIN dwh_staging.nami_ads ad ON
	eo.utm_medium = ad.utm_medium
	AND ad.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_ad_groups adgs ON
	adgs.id = ad.nami_ad_group_id
	AND adgs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_campaigns cps ON
	cps.id = adgs.nami_campaign_id
	AND cps.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_channel_courses chcs ON
	chcs.id = cps.nami_channel_course_id
	AND chcs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_channel_course_users ccus ON
	ccus.nami_channel_course_id = chcs.id
	AND ccus.deleted_at IS NULL
	AND TO_DATE(eo.created_at) >= TO_DATE(ccus.start_date)
	AND TO_DATE(eo.created_at)<= TO_DATE(ccus.end_date)
LEFT JOIN dwh_staging.nami_users usr ON
	usr.id = ccus.nami_user_id
	AND usr.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_group_users gusrs ON
	gusrs.nami_user_id = usr.id
	AND gusrs.deleted_at IS NULL
	AND TO_DATE(eo.created_at) >= TO_DATE(gusrs.start_date)
	AND TO_DATE(eo.created_at)<= TO_DATE(gusrs.end_date)
WHERE
    (eo.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (adgs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (cps.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (chcs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (ccus.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (usr.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (gusrs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
    OR (ad.transformed_at BETWEEN mbi.start_time AND mbi.end_time)