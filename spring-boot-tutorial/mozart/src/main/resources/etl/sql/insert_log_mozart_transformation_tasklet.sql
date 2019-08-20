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
INSERT INTO dwh_staging.log_mozart_transformation
SELECT
  uuid()               AS id
  ,mbi.start_time      AS start_time
  ,mbi.end_time        AS end_time
  ,now()               AS create_at
  ,now()               AS updated_at
FROM
   mozart_batch_information AS mbi