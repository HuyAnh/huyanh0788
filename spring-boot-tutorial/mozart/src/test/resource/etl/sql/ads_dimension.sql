(SELECT CONCAT('ads_', CAST(ads.id as String)) AS id,
ads.name AS ad_name,ads.tracking_link AS ad_link_tracking,adaccounts.name AS ad_account,channels.code AS ad_channel,
marketers.email AS ad_created_by,ad_groups.name AS ad_group,manager.email AS ad_team_lead,
(CASE WHEN department.type = 'Division' THEN department.name END) AS department,
(CASE WHEN groups.type = 'Group' THEN groups.name END) AS group_name FROM edumall_lake.ads
LEFT JOIN edumall_lake.ad_bydates ON (ads.id = ad_bydates.ad_id AND ads.deleted_at IS NULL) 
LEFT JOIN edumall_lake.ad_groups ON (ad_groups.id = ads.ad_group_id AND ad_groups.deleted_at IS NULL) 
LEFT JOIN edumall_lake.campaigns ON (campaigns.id = ad_groups.campaign_id AND campaigns.deleted_at IS NULL) 
LEFT JOIN edumall_lake.channel_course_users ON (channel_course_users.channel_course_id = campaigns.channel_course_id 
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') >= from_timestamp(channel_course_users.start_date,'yyyy/MM/dd')
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') < from_timestamp(channel_course_users.end_date,'yyyy/MM/dd')
AND channel_course_users.deleted_at IS NUll)
LEFT JOIN edumall_lake.marketers ON (marketers.id = channel_course_users.user_id AND marketers.deleted_at IS NUll) 
LEFT JOIN edumall_lake.channel_courses ON (channel_courses.id = campaigns.channel_course_id AND channel_courses.deleted_at IS NUll) 
LEFT JOIN edumall_lake.channels ON (channels.id = channel_courses.channel_id AND channels.deleted_at IS NUll)
LEFT JOIN edumall_lake.adaccounts ON (adaccounts.id = campaigns.adaccount_id AND adaccounts.deleted_at IS NUll) 
LEFT JOIN edumall_lake.mar_teams_marketers AS group_users ON (group_users.user_id = marketers.id
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') >= from_timestamp(group_users.start_date,'yyyy/MM/dd') 
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') < from_timestamp(group_users.end_date,'yyyy/MM/dd')
AND group_users.deleted_at IS NUll)
LEFT JOIN edumall_lake.mar_departments AS groups ON (groups.id = group_users.group_id AND groups.deleted_at IS NUll) 
LEFT JOIN edumall_lake.mar_departments AS department ON (department.id = groups.sup_department_id AND department.deleted_at IS NUll) 
LEFT JOIN edumall_lake.marketers AS manager ON (manager.id = groups.manager_id AND manager.deleted_at IS NUll)
WHERE ad_bydates.ad_id IS NULL) UNION (SELECT CAST(ad_bydates.id AS VARCHAR) AS id, ads.name AS ad_name, ads.tracking_link AS ad_link_tracking,
adaccounts.name AS ad_account,channels.code AS ad_channel,marketers.email AS ad_created_by,ad_groups.name AS ad_group,
manager.email  AS ad_team_lead,(CASE WHEN department.type = 'Division' THEN department.name END) AS department,
(CASE WHEN groups.type = 'Group' THEN groups.name END) AS group_name FROM edumall_lake.ad_bydates
LEFT JOIN edumall_lake.ads ON (ads.id = ad_bydates.ad_id AND ads.deleted_at IS NUll)
LEFT JOIN edumall_lake.ad_groups ON (ad_groups.id = ads.ad_group_id AND ad_groups.deleted_at IS NUll) 
LEFT JOIN edumall_lake.campaigns ON (campaigns.id = ad_groups.campaign_id AND campaigns.deleted_at IS NUll)
LEFT JOIN edumall_lake.channel_course_users ON (channel_course_users.channel_course_id = campaigns.channel_course_id 
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') >= from_timestamp(channel_course_users.start_date,'yyyy/MM/dd') 
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') < from_timestamp(channel_course_users.end_date,'yyyy/MM/dd') AND channel_course_users.deleted_at IS NUll) 
LEFT JOIN edumall_lake.marketers ON (marketers.id = channel_course_users.user_id AND marketers.deleted_at IS NUll)
LEFT JOIN edumall_lake.channel_courses ON (channel_courses.id = campaigns.channel_course_id AND channel_courses.deleted_at IS NUll) 
LEFT JOIN edumall_lake.channels ON (channels.id = channel_courses.channel_id AND channels.deleted_at IS NUll)
LEFT JOIN edumall_lake.adaccounts ON (adaccounts.id = campaigns.adaccount_id AND adaccounts.deleted_at IS NUll) 
LEFT JOIN edumall_lake.mar_teams_marketers AS group_users ON (group_users.user_id = marketers.id
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') >= from_timestamp(group_users.start_date,'yyyy/MM/dd') 
AND from_timestamp(ad_bydates.bydate,'yyyy/MM/dd') < from_timestamp(group_users.end_date,'yyyy/MM/dd') AND group_users.deleted_at IS NUll) 
LEFT JOIN edumall_lake.mar_departments AS groups ON (groups.id = group_users.group_id AND groups.deleted_at IS NUll)
LEFT JOIN edumall_lake.mar_departments AS department ON (department.id = groups.sup_department_id AND department.deleted_at IS NUll) 
LEFT JOIN edumall_lake.marketers AS manager ON (manager.id = groups.manager_id AND manager.deleted_at IS NUll))
