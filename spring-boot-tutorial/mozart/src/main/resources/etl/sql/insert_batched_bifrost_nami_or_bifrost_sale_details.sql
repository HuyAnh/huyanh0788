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
    , CASE
        WHEN vrho.`level` = 'L8' AND vrbt.status = 'success'
            THEN vrbt.received_at
        ELSE vrho.created_at
    END                                             AS date_lx
    , es.eros_group_work_id                         AS sub_department_sale_id
    , vrho.eros_staff_id                            AS staff_id
    , es.`role`                                     AS tvts_role_code
    , ec.sub_department_id                          AS sub_department_mkter_id
    , ec.mkter_id                                   AS mkter_id
    , COALESCE(ec.course_id, bc.id_edumall)         AS course_id
    , vrbt.method                                   AS payment_method_code
    , 1                                             AS country_id
    , vrbt.status                                   AS payment_status_code
    , CASE
        WHEN ec.combo_code IS NOT NULL AND ec.combo_code <> ''
            THEN 'COMBO'
        ELSE 'COURSE'
    END                                             AS target
    , ec.combo_code                                 AS combo_code
    , vrbt.gateway_partner                          AS partner_id
    , ec.channel_ad_id                              AS channel_ad_id
    , ec.source                                     AS source
    , NULL                                          AS reason_not_buy_id
    , vrho.order_status                             AS order_status
    , CASE
        WHEN vrho.level IS NOT NULL AND vrho.level <> ''
            THEN vrho.level
        ELSE 'L8'
    END                                             AS level
    , CAST((CASE
        WHEN (vrho.`level` = 'L8' AND vrbt.status = 'success') OR vrho.`level` IS NULL OR vrho.level = ''
            THEN bc.price
        ELSE 0
    END) AS FLOAT)                                  AS revenue
    , 0                                             AS number_call
    , 0                                             AS duration
    , 'true'                                        AS grouped_by_course
    , mbi.end_time                                  AS transformed_at
    , ec.ad_id                                      AS ad_id
    , ec.campaign_id                                AS campaign_id
    , ec.ad_account_id                              AS ad_account_id
    , hd.id                                         AS landing_page_id
    , ec.auto_cod_status                            AS auto_cod_status
    , 'bifrost'                                     AS source_system
FROM
    dwh_staging.edumall_contactc3s                      AS ec
    ,mozart_batch_information                        AS mbi
    LEFT JOIN dwh_staging.bifrost_transactions       AS vrbt
		ON CAST(vrbt.id AS string) = ec.id AND vrbt.`method` = 'gateway' AND vrbt.status = 'success'
    LEFT JOIN dwh_staging.ranked_history_orders       AS vrho
        ON vrho.eros_order_id = vrbt.siren_id
    LEFT JOIN dwh_staging.eros_staffs                   AS es
        ON es.id = vrho.eros_staff_id
    LEFT JOIN dwh_staging.bifrost_courses               AS bc
        ON bc.bifrost_transaction_id = vrbt.id
    LEFT JOIN dwh_staging.hera_domains hd
		ON ec.landing_page_domain = hd.name
WHERE
    ec.id IN (SELECT contactc3_id FROM dwh_staging.v_batched_bifrost_nami_or_bifrost_contactc3_ids)
