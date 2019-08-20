SELECT
	date_c3_id,
	date_lx_id,
	sub_department_sale_id,
	staff_id,
	tvts_role_code,
	sub_department_mkter_id,
	mkter_id,
	course_id,
	payment_method_code,
	country_id,
	payment_status_code,
	combo_code,
	target,
	partner_id,
	channel_ad_id,
	source_contactc3_code,
	reason_not_buy_id,
	sale_method_id,
	SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level` IS NULL THEN a.number_c3 ELSE 0 END) number_c3,
	SUM(CASE WHEN a.grouped_by_course = 'true'  AND a.`level` IS NULL THEN a.number_c3 ELSE 0 END) number_c3_c,
	SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L1' THEN a.number_c3 ELSE 0 END) number_l1,
    SUM(CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L1' THEN a.number_c3 ELSE 0 END) number_l1_c,
    SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L2' THEN a.number_c3 ELSE 0 END) number_l2,
    SUM(CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L2' THEN a.number_c3 ELSE 0 END) number_l2_c,
    SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L3' THEN a.number_c3 ELSE 0 END) number_l3,
    SUM(CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L3' THEN a.number_c3 ELSE 0 END) number_l3_c,
    SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L4' THEN a.number_c3 ELSE 0 END) number_l4,
    SUM(CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L4' THEN a.number_c3 ELSE 0 END) number_l4_c,
    SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L7' THEN a.number_c3 ELSE 0 END) number_l7,
    SUM(CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L7' THEN a.number_c3 ELSE 0 END) number_l7_c,
    SUM(CASE WHEN a.grouped_by_course = 'false' AND a.`level`='L8' THEN a.number_c3 ELSE 0 END) number_l8,
    SUM(CASE WHEN a.grouped_by_course = 'true' AND a.`level`='L8' THEN a.number_c3 ELSE 0 END) number_l8_c,
	SUM(a.number_call) number_call,
	SUM(a.duration) duration,
	SUM(a.number_order_canceled) number_order_canceled,
	SUM(CASE WHEN a.grouped_by_course = 'false' THEN a.revenue ELSE 0 END) revenue_before_control,
    SUM(CASE WHEN a.grouped_by_course = 'true' THEN a.revenue ELSE 0 END) revenue_before_control_c,
	0 revenue_after_control
FROM(

SELECT
cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer)  date_c3_id,
NULL  date_lx_id,
NULL sub_department_sale_id,
NULL staff_id,
NULL tvts_role_code,
sd.sub_department_mkter_id sub_department_mkter_id,
sd.mkter_id mkter_id,
sd.course_id course_id,
NULL payment_method_code,
sd.country_id country_id,
sd.payment_status_code,
sd.combo_code combo_code,
sd.target,
NULL partner_id,
sd.channel_ad_id channel_ad_id,
sd.`source` source_contactc3_code,
NULL reason_not_buy_id,
NULL `level`,
sd.grouped_by_course,
CASE
	WHEN sd.auto_cod_status = 'done'
		THEN 1
	WHEN sd.auto_cod_status = 'done'
		THEN 2
	WHEN COALESCE(sd.auto_cod_status,'b') NOT IN ('done')
		THEN 3
	WHEN sd.payment_method_code = 'gateway' AND sd.payment_status_code = 'success'
		THEN 4
	ELSE 0
END sale_method_id,
COUNT (DISTINCT sd.contactc3_id) number_c3,
0 number_call,
0 duration,
0 number_order_canceled,
0 revenue
 FROM  dwh_staging.sale_details sd
 WHERE cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) in ?1

 GROUP BY cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer),
 sd.sub_department_mkter_id,
 sd.mkter_id,
sd.course_id,
sd.country_id ,
sd.payment_status_code,
sd.combo_code,
sd.target,
sd.channel_ad_id,
sd.`source`,
sd.grouped_by_course,
CASE
	WHEN sd.auto_cod_status = 'done'
		THEN 1
	WHEN sd.auto_cod_status = 'done'
		THEN 2
	WHEN COALESCE(sd.auto_cod_status,'b') NOT IN ('done')
		THEN 3
	WHEN sd.payment_method_code = 'gateway' AND sd.payment_status_code = 'success'
		THEN 4
	ELSE 0
END


 UNION ALL


SELECT
    cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer)  date_c3_id,
	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_lx),'yyyyMMdd')) as integer)  date_lx_id,
    sd.sub_department_sale_id,
    sd.staff_id,
    sd.tvts_role_code,
    sd.sub_department_mkter_id,
    sd.mkter_id,
    sd.course_id,
    sd.payment_method_code,
    sd.country_id,
    sd.payment_status_code,
    sd.combo_code,
    sd.target,
    sd.partner_id,
    sd.channel_ad_id,
    sd.`source` source_contactc3_code,
    sd.reason_not_buy_id,
    sd.`level`,
    sd.grouped_by_course,
    CASE
	WHEN sd.auto_cod_status = 'done' AND sd.staff_id = '5a8f895043ae5f000aad1316'
		THEN 1
	WHEN sd.auto_cod_status = 'done' AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 2
	WHEN COALESCE(sd.auto_cod_status,'b') NOT IN ('done') AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 3
	WHEN sd.payment_method_code = 'gateway'
		THEN 4
	ELSE 0
	END sale_method_id,
    COUNT (DISTINCT sd.contactc3_id) number_c3,
    SUM(sd.number_call) number_call,
	SUM(sd.duration) duration,
	0 number_order_canceled,
	SUM(sd.revenue) revenue
FROM  dwh_staging.sale_details sd
 WHERE cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) in ?1
GROUP BY
cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) ,
   cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_lx),'yyyyMMdd')) as integer),
    sd.sub_department_sale_id,
    sd.staff_id,
    sd.tvts_role_code,
    sd.sub_department_mkter_id,
    sd.mkter_id,
    sd.course_id,
    sd.payment_method_code,
    sd.country_id,
    sd.payment_status_code,
    sd.combo_code,
    sd.target,
    sd.partner_id,
    sd.channel_ad_id,
    sd.`source`,
    sd.reason_not_buy_id,
    sd.`level`,
    sd.grouped_by_course,
    CASE
	WHEN sd.auto_cod_status = 'done' AND sd.staff_id = '5a8f895043ae5f000aad1316'
		THEN 1
	WHEN sd.auto_cod_status = 'done' AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 2
	WHEN COALESCE(sd.auto_cod_status,'b') NOT IN ('done') AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 3
	WHEN sd.payment_method_code = 'gateway'
		THEN 4
	ELSE 0
	END

 UNION ALL

 SELECT
    cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer)  date_c3_id,
   cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_lx),'yyyyMMdd')) as integer)  date_lx_id,
    sd.sub_department_sale_id,
    sd.staff_id,
    sd.tvts_role_code,
    sd.sub_department_mkter_id,
    sd.mkter_id,
    sd.course_id,
    sd.payment_method_code,
    sd.country_id,
    sd.payment_status_code,
    sd.combo_code,
    sd.target,
    sd.partner_id,
    sd.channel_ad_id,
    sd.`source` source_contactc3_code,
    sd.reason_not_buy_id,
    NULL `level`,
    NULL grouped_by_course,
    CASE
	WHEN sd.auto_cod_status = 'done' AND sd.staff_id = '5a8f895043ae5f000aad1316'
		THEN 1
	WHEN sd.auto_cod_status = 'done' AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 2
	WHEN COALESCE(sd.auto_cod_status,'b') NOT IN ('done') AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 3
	WHEN sd.payment_method_code = 'gateway'
		THEN 4
	ELSE 0
	END sale_method_id,
   	0 number_c3,
    0 number_call,
	0 duration,
	 COUNT (DISTINCT sd.contactc3_id) number_order_canceled,
	0 revenue
FROM  dwh_staging.sale_details sd
 WHERE sd.order_status = 'cancelled' AND grouped_by_course = 'false' AND cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) in ?1
GROUP BY
	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_c3),'yyyyMMdd')) as integer) ,
	cast((FROM_UNIXTIME(UNIX_TIMESTAMP(sd.date_lx),'yyyyMMdd')) as integer),
    sd.sub_department_sale_id,
    sd.staff_id,
    sd.tvts_role_code,
    sd.sub_department_mkter_id,
    sd.mkter_id,
    sd.course_id,
    sd.payment_method_code,
    sd.country_id,
    sd.payment_status_code,
    sd.combo_code,
    sd.target,
    sd.partner_id,
    sd.channel_ad_id,
    sd.`source`,
    sd.reason_not_buy_id,
    CASE
	WHEN sd.auto_cod_status = 'done' AND sd.staff_id = '5a8f895043ae5f000aad1316'
		THEN 1
	WHEN sd.auto_cod_status = 'done' AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 2
	WHEN COALESCE(sd.auto_cod_status,'b') NOT IN ('done') AND COALESCE(sd.staff_id,'a') NOT IN ('5a8f895043ae5f000aad1316')
		THEN 3
	WHEN sd.payment_method_code = 'gateway'
		THEN 4
	ELSE 0
	END

 ) a GROUP BY
	 date_c3_id,
	date_lx_id,
	sub_department_sale_id,
	staff_id,
	tvts_role_code,
	sub_department_mkter_id,
	mkter_id,
	course_id,
	payment_method_code,
	country_id,
	payment_status_code,
	combo_code,
	target,
	partner_id,
	channel_ad_id,
	source_contactc3_code,
	reason_not_buy_id,
	sale_method_id;