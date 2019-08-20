SELECT (CASE
            WHEN groups.type = 'Group' THEN cast(groups.id AS string)
        END) AS sub_department_id,
       (CASE
            WHEN groups.type = 'Group' THEN groups.name
        END) AS sub_department_name,
       (CASE
            WHEN department.type = 'Division' THEN department.id
        END) AS department_id,
       (CASE
            WHEN department.type = 'Division' THEN department.name
        END) AS department_name,
       'MKTER' "type"
FROM dwh_staging.nami_departments AS groups
JOIN
  (SELECT mozart_transformation.info
   FROM dwh_staging.mozart_transformation
   WHERE name = 'end_time') et
JOIN
  (SELECT mozart_transformation.info
   FROM dwh_staging.mozart_transformation
   WHERE name = 'start_time') st
JOIN dwh_staging.nami_departments AS department ON department.id = groups.sup_department_id
WHERE (st.info IS NULL
       OR groups.transformed_at BETWEEN st.info AND et.info) AND groups.type = 'Group'
UNION
SELECT '1000' AS sub_department_id,
       'Combo' AS sub_department_name,
       1000 AS depatment_id,
       'Combo' AS department_name,
       'MKTER' AS TYPE
UNION
SELECT '2001' AS sub_department_id,
       'Ecom Web' AS sub_department_name,
       2000 AS department_id,
       'Ecom' AS department_name,
       'MKTER' AS TYPE
UNION
SELECT '2002' AS sub_department_id,
       'Ecom App' AS sub_department_name,
       2000 AS department_id,
       'Ecom' AS department_name,
       'MKTER' AS TYPE
UNION ALL
SELECT id AS sub_department_id,
       name AS sub_department_name ,
       CASE
           WHEN trim(name) IN ('Training',
                               'Telesale',
                               'Telemarketer',
                               'Temp Support',
                               'CTV Telesale',
                               'Nhánh C',
                               'Nhánh D',
                               'Nhánh B',
                               'Nhánh A',
                               'Affersales Support',
                               'HoaPT',
                               'Leadinhouse',
                               'Backup autocod_ outsource',
                               'C3 Tele') THEN 1
           WHEN trim(name) IN ('Hoa sao',
                               'Đối tác Only',
                               'Đối tác NMS',
                               'DN',
                               'Diamond Star',
                               'Backup Outsource',
                               'Teamlead Outsource',
                               'Niềm tin Việt',
                               'Expertrain',
                               'HEKA',
                               'KASACO',
                               'Vĩnh Thịnh',
                               'MOCAP',
                               'Outsource_Autocod',
                               'XTS',
                               'MTA',
                               'D$N Combo',
                               'Minh Phúc',
                               'Minh Phúc AutoCod',
                               'NMS Auto',
                               'NMS AutoCOD') THEN 2
           WHEN trim(name) IN ('Support sale',
                               'Team Back up',
                               'Convert sale',
                               'L7.3 team') THEN 3
           WHEN trim(name) IN ('AutoCOD của hệ thống',
                               'Autosale',
                               'Automation',
                               'Auto sales',
                               'AutoCOD',
                               'Nhóm VANNTP') THEN 4
           ELSE 5
       END department_id,
       CASE
           WHEN trim(name) IN ('Training',
                               'Telesale',
                               'Telemarketer',
                               'Temp Support',
                               'CTV Telesale',
                               'Nhánh C',
                               'Nhánh D',
                               'Nhánh B',
                               'Nhánh A',
                               'Affersales Support',
                               'HoaPT',
                               'Leadinhouse',
                               'Backup autocod_ outsource',
                               'C3 Tele') THEN 'Inhouse'
           WHEN trim(name) IN ('Hoa sao',
                               'Đối tác Only',
                               'Đối tác NMS',
                               'DN',
                               'Diamond Star',
                               'Backup Outsource',
                               'Teamlead Outsource',
                               'Niềm tin Việt',
                               'Expertrain',
                               'HEKA',
                               'KASACO',
                               'Vĩnh Thịnh',
                               'MOCAP',
                               'Outsource_Autocod',
                               'XTS',
                               'MTA',
                               'D$N Combo',
                               'Minh Phúc',
                               'Minh Phúc AutoCod',
                               'NMS Auto',
                               'NMS AutoCOD') THEN 'Outsource'
           WHEN trim(name) IN ('Support sale',
                               'Team Back up',
                               'Convert sale',
                               'L7.3 team') THEN 'L7.3'
           WHEN trim(name) IN ('AutoCOD của hệ thống',
                               'Autosale',
                               'Automation',
                               'Auto sales',
                               'AutoCOD',
                               'Nhóm VANNTP') THEN 'AutoCOD'
           ELSE 'Khác'
       END department_name ,
       'SALE' "type"
FROM dwh_staging.eros_group_works egw
JOIN
  (SELECT mozart_transformation.info
   FROM dwh_staging.mozart_transformation
   WHERE name = 'end_time') et
JOIN
  (SELECT mozart_transformation.info
   FROM dwh_staging.mozart_transformation
   WHERE name = 'start_time') st
WHERE (st.info IS NULL
       OR egw.transformed_at BETWEEN st.info AND et.info);