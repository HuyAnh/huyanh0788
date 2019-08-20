SELECT
	date_c3_id,
	date_lx_id,
	mkter_id,
	sub_department_mkter_id,
	course_id,
	combo_code,
	target,
	country_id,
	ad_id,
	channel_ad_id,
	ad_account_id,
	campaign_id,
	landing_page_id,
	source_contactc3_code,
	sum(c1) AS number_c1,
	sum(c2) AS number_c2,
	sum(cost) AS cost,
	SUM(
		CASE WHEN a.grouped_by_course = 'false' AND a.`level` IS NULL
			THEN a.number_c3
		ELSE 0 END) number_c3,
	SUM(
		CASE WHEN a.grouped_by_course = 'true'  AND a.`level` IS NULL
			THEN a.number_c3
		ELSE 0 END) number_c3_c,
	SUM(
		CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L1'
			THEN a.number_c3
		ELSE 0 END) number_l1,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L1'
    		THEN a.number_c3
    	ELSE 0 END) number_l1_c,
    SUM(
    	CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L2'
    		THEN a.number_c3
    	ELSE 0 END) number_l2,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L2'
    		THEN a.number_c3
    	ELSE 0 END) number_l2_c,
    SUM(
    	CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L3'
    		THEN a.number_c3
    	ELSE 0 END) number_l3,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L3'
    		THEN a.number_c3
		ELSE 0 END) number_l3_c,
    SUM(
    	CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L4'
    		THEN a.number_c3
    	ELSE 0 END) number_l4,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L4'
    		THEN a.number_c3
    	ELSE 0 END) number_l4_c,
    SUM(
    	CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L7'
    		THEN a.number_c3
    	ELSE 0 END) number_l7,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L7'
    		THEN a.number_c3
    	ELSE 0 END) number_l7_c,
    SUM(
    	CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L8'
    		THEN a.number_c3
    	ELSE 0 END) number_l8,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L8'
    		THEN a.number_c3
    	ELSE 0 END) number_l8_c,
	SUM(
		CASE WHEN a.grouped_by_course = 'false'
			THEN a.revenue
		ELSE 0 END) revenue_before_control,
    SUM(
    	CASE WHEN a.grouped_by_course = 'true'
    		THEN a.revenue
    	ELSE 0 END) revenue_before_control_c,
	0 revenue_after_control
FROM(
SELECT
cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer)  date_c3_id,
NULL  date_lx_id,
sd.mkter_id,
sd.sub_department_mkter_id,
sd.course_id,
sd.combo_code,
sd.target,
sd.country_id,
sd.ad_id,
sd.channel_ad_id,
sd.ad_account_id,
sd.campaign_id,
sd.landing_page_id,
sd.`source` source_contactc3_code,
NULL `level`,
sd.grouped_by_course,
0 c1,
0 c2,
0 cost,
COUNT (DISTINCT sd.contactc3_id) number_c3,
0 revenue
FROM  dwh_staging.sale_details sd
WHERE cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) in ?1
GROUP BY
cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer),
sd.mkter_id,
sd.sub_department_mkter_id,
sd.course_id,
sd.combo_code,
sd.target,
sd.country_id,
sd.ad_id,
sd.channel_ad_id,
sd.ad_account_id,
sd.campaign_id,
sd.landing_page_id,
sd.`source`,
sd.grouped_by_course
 UNION ALL
SELECT
    cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer)  date_c3_id,
	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_lx),'yyyyMMdd')) as integer)  date_lx_id,
    sd.mkter_id,
	sd.sub_department_mkter_id,
	sd.course_id,
	sd.combo_code,
	sd.target,
	sd.country_id,
	sd.ad_id,
	sd.channel_ad_id,
	sd.ad_account_id,
	sd.campaign_id,
	sd.landing_page_id,
	sd.`source` source_contactc3_code,
    sd.`level`,
    sd.grouped_by_course,
    0 c1,
    0 c2,
    0 cost,
    COUNT (DISTINCT sd.contactc3_id) number_c3,
	SUM(sd.revenue) revenue
FROM  dwh_staging.sale_details sd
 WHERE cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) in ?1
GROUP BY
	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) ,
   	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_lx),'yyyyMMdd')) as integer),
    sd.mkter_id,
	sd.sub_department_mkter_id,
	sd.course_id,
	sd.combo_code,
	sd.target,
	sd.country_id,
	sd.ad_id,
	sd.channel_ad_id,
	sd.ad_account_id,
	sd.campaign_id,
	sd.landing_page_id,
	sd.`source`,
    sd.`level`,
    sd.grouped_by_course
 UNION ALL
 SELECT
	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(abd.bydate),'yyyyMMdd')) as integer)  date_c3_id,
	NULL date_lx_id,
	ccus.nami_user_id mkter_id,
	gusrs.nami_group_id AS  sub_department_mkter_id,
	jcs.id AS course_id,
	jsp.code AS combo_code,
	CASE
		WHEN jsp.code IS NOT NULL
			AND jsp.code <> '' THEN 'COMBO'
		ELSE 'COURSE'
		END target,
	1 AS country_id,
	abd.ad_id,
	chcs.nami_channel_id AS channel_ad_id,
	cp.nami_adaccount_id AS ad_account_id,
	adg.nami_campaign_id AS campaign_id,
	0 AS landing_page_id,
	NULL AS source_contactc3_code,
	NULL `level`,
	NULL grouped_by_course,
	abd.c1,
	abd.c2,
	abd.cost,
	0 number_c3,
	0 revenue

FROM dwh_staging.nami_ad_bydates abd
LEFT JOIN dwh_staging.nami_ads ad
	ON ad.id = abd.ad_id
	AND ad.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_ad_groups adg
	ON adg.id = ad.nami_ad_group_id
	AND adg.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_campaigns cp
	ON cp.id = adg.nami_campaign_id
	AND cp.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_channel_courses chcs
	ON chcs.id = cp.nami_channel_course_id
	AND chcs.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_courses ncs
	ON ncs.id = chcs.nami_course_id
	AND ncs.deleted_at IS NULL
LEFT JOIN dwh_staging.jackfruit_courses jcs
	ON jcs.id = ncs.source_id
LEFT JOIN (
			SELECT
				DISTINCT code
			FROM  dwh_staging.jackfruit_sale_packages
			WHERE code IS NOT NULL
		) jsp
		ON jsp.code = ncs.source_id
LEFT JOIN dwh_staging.nami_channel_course_users ccus
	ON ccus.nami_channel_course_id = chcs.id
	AND ccus.deleted_at IS NULL
	AND TO_DATE(abd.bydate) >= TO_DATE(ccus.start_date)
	AND TO_DATE(abd.bydate)<= TO_DATE(ccus.end_date)
LEFT JOIN dwh_staging.nami_users usr
	ON usr.id = ccus.nami_user_id
	AND usr.deleted_at IS NULL
LEFT JOIN dwh_staging.nami_group_users gusrs
	ON gusrs.nami_user_id =usr.id
	AND gusrs.deleted_at IS NULL
	AND TO_DATE(abd.bydate) >= TO_DATE(gusrs.start_date)
	AND TO_DATE(abd.bydate)<= TO_DATE(gusrs.end_date)
WHERE cast((FROM_UNIXTIME(UNIX_TIMESTAMP(abd.bydate),'yyyyMMdd')) as integer) in ?2
 ) a GROUP BY
	date_c3_id,
	date_lx_id,
	mkter_id,
	sub_department_mkter_id,
	course_id,
	combo_code,
	target,
	country_id,
	ad_id,
	channel_ad_id,
	ad_account_id,
	campaign_id,
	landing_page_id,
	source_contactc3_code