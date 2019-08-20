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
SELECT distinct cast((FROM_UNIXTIME(UNIX_TIMESTAMP(nab.bydate), 'yyyyMMdd')) AS integer) date_c3_id
FROM dwh_staging.nami_ad_bydates AS nab,
     mozart_batch_information AS mbi
WHERE nab.transformed_at BETWEEN mbi.start_time AND mbi.end_time;