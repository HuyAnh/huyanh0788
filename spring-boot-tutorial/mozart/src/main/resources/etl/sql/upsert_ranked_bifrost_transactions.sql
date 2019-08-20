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
UPSERT INTO dwh_staging.ranked_bifrost_transactions
SELECT id,
       METHOD,
       price,
       status,
       bifrost_buyer_id,
       created_at,
       updated_at,
       combo,
       SOURCE,
       siren_id,
       received_at,
       source_url,
       note,
       reason_fail,
       response_partner,
       gateway_partner,
       transformed_at,
       status_ranked
FROM
  (SELECT id,
          METHOD,
          price,
          status,
          bifrost_buyer_id,
          created_at,
          updated_at,
          combo,
          SOURCE,
          siren_id,
          received_at,
          source_url,
          note,
          reason_fail,
          response_partner,
          gateway_partner,
          transformed_at,
          CASE
              WHEN status = 'success' THEN 1
              WHEN status = 'pending' THEN 2
              WHEN status = 'failed' THEN 3
              WHEN status = 'canceled' THEN 4
              ELSE 5
          END status_ranked,
          row_number() OVER (PARTITION BY siren_id
                             ORDER BY CASE
                                          WHEN status = 'success' THEN 1
                                          WHEN status = 'pending' THEN 2
                                          WHEN status = 'failed' THEN 3
                                          WHEN status = 'canceled' THEN 4
                                          ELSE 5
                                      END ASC, created_at DESC) ranked
   FROM dwh_staging.bifrost_transactions) rbt,
    mozart_batch_information mbi
WHERE (rbt.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  AND ranked = 1