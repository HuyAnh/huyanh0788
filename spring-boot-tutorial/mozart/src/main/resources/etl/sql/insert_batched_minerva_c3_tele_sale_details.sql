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
    uuid()                                              AS id
    , ec.id                                             AS contactc3_id
    , ec.created_at                                     AS date_c3
    , CASE
        WHEN vrho.`level` = 'L8' AND vrbt.status = 'success'
            THEN vrbt.received_at
        ELSE vrho.created_at
    END                                                 AS date_lx
    , '1000000'                                         AS sub_department_sale_id
    , vrho.eros_staff_id                                AS staff_id
    , es.`role`                                         AS tvts_role_code
    , ec.sub_department_id                              AS sub_department_mkter_id
    , ec.mkter_id                                       AS mkter_id
    , COALESCE(ec.course_id, bc.id_edumall)             AS course_id
    , vrbt.method                                       AS payment_method_code
    , 1                                                 AS country_id
    , vrbt.status                                       AS payment_status_code
    , 'COMBO'                                           AS target
    , ec.combo_code                                     AS combo_code
    , COALESCE(`go`.partner_id, vrbt.gateway_partner)   AS partner_id
    , ec.channel_ad_id                                  AS channel_ad_id
    , ec.source                                         AS source
    , ernb.id                                           AS reason_not_buy_id
    , vrho.order_status                                 AS order_status
    , vrho.level                                        AS level
    , CAST((CASE
        WHEN (vrho.`level` = 'L8' AND vrbt.status = 'success')
            THEN bc.price
        ELSE 0
    END) AS FLOAT)                                      AS revenue
    , vgecl.number_call                                 AS number_call
    , vgecl.duration                                    AS duration
    , 'true'                                            AS grouped_by_course
    , mbi.end_time                                      AS transformed_at
    , ec.ad_id                                          AS ad_id
    , ec.campaign_id                                    AS campaign_id
    , ec.ad_account_id                                  AS ad_account_id
    , hd.id                                             AS landing_page_id
    , ec.auto_cod_status                                AS auto_cod_status
    , 'minerva_c3_tele'                                 AS source_system
FROM
    dwh_staging.edumall_contactc3s                      AS ec
    ,mozart_batch_information                        AS mbi
    LEFT JOIN dwh_staging.kitty_order_caches            AS koc1
        ON ec.id = koc1.contactc3_source_id
    LEFT JOIN dwh_staging.kitty_order_courses           AS koc2
        ON koc2.order_id = koc1.id
    LEFT JOIN dwh_staging.ranked_history_orders       AS vrho
        ON vrho.eros_order_id = koc2.eros_order_id
    LEFT JOIN dwh_staging.ranked_bifrost_transactions AS vrbt
        ON vrbt.siren_id = koc2.eros_order_id
    LEFT JOIN dwh_staging.bifrost_courses               AS bc
        ON bc.bifrost_transaction_id = vrbt.id
    LEFT JOIN dwh_staging.eros_staffs                   AS es
        ON es.id = vrho.eros_staff_id
    LEFT JOIN dwh_staging.ranked_gambit_orders          AS `go`
        ON `go`.siren_id = koc2.eros_order_id
        AND strleft(`go`.level, 2) = vrho.level
    LEFT JOIN dwh_staging.eros_reason_not_buy           AS ernb
        ON ernb.reason_not_buy = vrho.reason_not_buy
    LEFT JOIN dwh_staging.v_grouped_eros_call_logs      AS vgecl
        ON vgecl.eros_order_id = vrho.eros_order_id AND vgecl.level_order = vrho.level
    LEFT JOIN dwh_staging.hera_domains hd
		ON ec.landing_page_domain = hd.name
WHERE
    ec.id IN (SELECT contactc3_id FROM dwh_staging.v_batched_minerva_c3_tele_contactc3_ids)
