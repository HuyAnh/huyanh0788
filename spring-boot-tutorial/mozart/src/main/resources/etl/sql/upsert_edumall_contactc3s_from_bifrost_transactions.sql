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
SELECT CAST(tss.id AS string) as id,
       bs.name,
       bs.mobile,
       bs.email,
       'bifrost_transaction' AS type,
       tss.source,
       tss.source_url,
       NULL AS strategy,
       NULL AS status,
       tss.created_at AS created_at,
       bfcs.id_edumall AS course_id,
       sps.code AS combo_code,
       NULL AS mkter_id,
       NULL AS sub_department_id,
       NULL AS ad_id,
       NULL AS channel_ad_id,
       NULL AS campaign_id,
       NULL AS ad_account_id,
       NULL AS ad_group_id,
       'Bifrost' AS source_system,
       mbi.end_time AS transformed_at,
       NULL AS auto_cod_process,
       NULL AS auto_cod_status,
       NULL AS cod,
       NULL AS form_id,
       split_part(tss.source_url, '/', 3) AS landing_page_domain
FROM dwh_staging.bifrost_transactions tss, mozart_batch_information mbi
LEFT JOIN dwh_staging.bifrost_buyers bs ON tss.bifrost_buyer_id = bs.id
LEFT JOIN
  (SELECT *
   FROM dwh_staging.bifrost_courses b
   WHERE EXISTS
       (SELECT 1
        FROM
          (SELECT bifrost_transaction_id,
                  COUNT(id_edumall) num_course
           FROM dwh_staging.bifrost_courses
           GROUP BY bifrost_transaction_id
           HAVING num_course = 1) a
        WHERE a.bifrost_transaction_id = b.bifrost_transaction_id) ) bfcs ON bfcs.bifrost_transaction_id = tss.id
AND tss.combo IS NULL
LEFT JOIN
  (SELECT DISTINCT code
   FROM dwh_staging.jackfruit_sale_packages) sps ON tss.combo = sps.code
WHERE (
  (tss.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  OR (bs.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
)
  AND tss.method = 'gateway'
  AND tss.status = 'success'
  AND (tss.siren_id IS  NULL OR tss.siren_id = '')
  AND NOT EXISTS
    (SELECT 1
     FROM dwh_staging.minerva_contactc3s m
     WHERE m.id = tss.siren_id)
  AND NOT EXISTS
    (SELECT 1
     FROM dwh_staging.nami_contactc3s nmcts
     WHERE nmcts.origin_from = 'bifrost_transaction'
       AND nmcts.origin_id = CAST(tss.id AS string) ) ;