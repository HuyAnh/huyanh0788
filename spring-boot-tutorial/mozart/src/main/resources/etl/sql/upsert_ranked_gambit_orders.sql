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
UPSERT INTO dwh_staging.ranked_gambit_orders
SELECT siren_id
, level
, partner_id
, order_at
, created_at
, updated_at
, transformed_at
FROM
(SELECT
siren_id
, strleft(level, 2) level
, partner_id
, order_at
, created_at
, updated_at
, transformed_at
, row_number() OVER (PARTITION BY siren_id, strleft(level, 2) ORDER BY order_at DESC) ranked
FROM dwh_staging.gambit_orders god) gog, mozart_batch_information mbi
WHERE (gog.transformed_at BETWEEN mbi.start_time AND mbi.end_time) AND ranked = 1