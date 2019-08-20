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
UPSERT INTO dwh_staging.ranked_history_orders
SELECT id,
       eros_order_id,
       created_at,
       updated_at,
       transformed_at,
       eros_staff_id,
       reason_not_buy,
       order_status,
       `level`
FROM
  (SELECT id,
          eros_order_id,
          created_at,
          updated_at,
          transformed_at,
          eros_staff_id,
          reason_not_buy,
          order_status,
          strleft(`level`, 2) `level`,
          row_number() OVER (PARTITION BY eros_order_id, strleft(`level`, 2) ORDER BY created_at ASC) ranked
   FROM dwh_staging.eros_history_orders) rhod, mozart_batch_information mbi
WHERE (rhod.transformed_at BETWEEN mbi.start_time AND mbi.end_time)
  AND ranked = 1