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
        WHEN vrkho.level_after_call = 'L8' AND vkt.status = 'success'
            THEN vkt.received_at
        ELSE vrkho.created_at
    END                                             AS date_lx
    , '1000000'                                     AS sub_department_sale_id
    , NULL                                          AS staff_id
    , NULL                                          AS tvts_role_code
    , ec.sub_department_id                          AS sub_department_mkter_id
    , ec.mkter_id                                   AS mkter_id
    , ec.course_id                                  AS course_id
    , vkt.method                                    AS payment_method_code
    , 1                                             AS country_id
    , vkt.status                                    AS payment_status_code
    , 'COMBO'                                       AS target
    , ec.combo_code                                 AS combo_code
    , COALESCE(`go`.partner_id, vkt.partner_id)     AS partner_id
    , ec.channel_ad_id                              AS channel_ad_id
    , ec.source                                     AS source
    , NULL                                          AS reason_not_buy_id
    , vrkho.care_status_key                         AS order_status
    , vrkho.level_after_call                        AS level
    , CAST(
        CASE
            WHEN vrkho.level_after_call = 'L8' AND vkt.status = 'success'
                THEN vkt.price
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
    , 'no_course_minerva_c3_tele'                   AS source_system
FROM
    dwh_staging.edumall_contactc3s                                  AS ec
    ,mozart_batch_information                                       AS mbi
    LEFT JOIN dwh_staging.kitty_order_caches                        AS koc1
        ON koc1.contactc3_source_id = ec.id
    LEFT JOIN dwh_staging.v_ranked_kitty_history_orders             AS vrkho
        ON vrkho.kitty_order_cache_id = koc1.id AND vrkho.ranked = 1
    LEFT JOIN dwh_staging.v_kitty_transactions                      AS vkt
        ON vkt.contactc3_source_id = ec.id
    LEFT JOIN dwh_staging.v_gambit_order_contactc3_c3tele           AS `go`
		ON `go`.contactc3_source_id = ec.id
		   AND `go`.`level` = vrkho.level_after_call
		   AND `go`.ranked = 1

    LEFT JOIN dwh_staging.hera_domains hd
		ON ec.landing_page_domain = hd.name
WHERE
    ec.id IN
        (
            SELECT
                contactc3_id
            FROM
                dwh_staging.v_batched_no_course_minerva_c3_tele_contactc3_ids
        );
