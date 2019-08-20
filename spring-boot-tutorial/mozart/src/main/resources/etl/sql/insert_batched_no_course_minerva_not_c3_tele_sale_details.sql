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
INSERT INTO
    dwh_staging.sale_details
SELECT
    uuid()                                          AS id
    , ec.id                                         AS contactc3_id
    , ec.created_at                                 AS date_c3
    , DECODE(vrho.level,
             'L8',
             vrbt.received_at,
             vrho.created_at)                       AS date_lx
    , es.eros_group_work_id                         AS sub_department_sale_id
    , CASE
         WHEN vrho.LEVEL IN ('L3','L4','L7','L8')
             THEN od.eros_staff_id
         ELSE vrho.eros_staff_id
      END                                           AS staff_id
    , es.`role`                                     AS tvts_role_code
    , ec.sub_department_id                          AS sub_department_mkter_id
    , ec.mkter_id                                   AS mkter_id
    , ec.course_id                                  AS course_id
    , vrbt.method                                   AS payment_method_code
    , 1                                             AS country_id
    , vrbt.status                                   AS payment_status_code
    , CASE
        WHEN ec.course_id  IS NOT NULL AND ec.course_id <> ''
            THEN 'COURSE'
        WHEN ec.combo_code IS NOT NULL AND ec.combo_code <> ''
            THEN 'COMBO'
        ELSE 'OTHER'
    END                                             AS target
    , ec.combo_code                                 AS combo_code
    , COALESCE(`go`.partner_id,
               vrbt.gateway_partner)                AS partner_id
    , ec.channel_ad_id                              AS channel_ad_id
    , ec.source                                     AS source
    , ernb.id                                       AS reason_not_buy_id
    , vrho.order_status                             AS order_status
    , vrho.level                                    AS level
    , CAST(
        CASE
            WHEN vrho.`level` = 'L8' AND vrbt.status = 'success'
                THEN vrbt.price
            ELSE 0
        END AS FLOAT
    )                                               AS revenue
    , 0                                             AS number_call
    , 0                                             AS duration
    , 'false'                                       AS grouped_by_course
    , mbi.end_time                                  AS transformed_at
    , ec.ad_id                                      AS ad_id
    , ec.campaign_id                                AS campaign_id
    , ec.ad_account_id                              AS ad_account_id
    , hd.id                                         AS landing_page_id
    , ec.auto_cod_status                            AS auto_cod_status
    , 'no_course_minerva_not_c3_tele'               AS source_system
FROM
    dwh_staging.edumall_contactc3s                                                          AS ec
    ,mozart_batch_information                                                             AS mbi
    LEFT JOIN dwh_staging.ranked_history_orders                                           AS vrho
        ON vrho.eros_order_id = ec.id
    LEFT JOIN dwh_staging.ranked_bifrost_transactions                                     AS vrbt
        ON vrbt.siren_id = ec.id
    LEFT JOIN dwh_staging.eros_staffs                                                       AS es
        ON es.id = vrho.eros_staff_id
    LEFT JOIN dwh_staging.ranked_gambit_orders                                              AS `go`
        ON `go`.siren_id = ec.id AND strleft(`go`.level,2) = vrho.level

    LEFT JOIN dwh_staging.eros_reason_not_buy AS ernb
        ON ernb.reason_not_buy = vrho.reason_not_buy
    LEFT JOIN dwh_staging.hera_domains hd
		ON ec.landing_page_domain = hd.name
	LEFT JOIN dwh_staging.eros_orders od
					ON od.id = ec.id
WHERE
    ec.id IN
        (
            SELECT
                contactc3_id
            FROM
                dwh_staging.v_batched_no_course_minerva_not_c3_tele_contactc3_ids
        )
