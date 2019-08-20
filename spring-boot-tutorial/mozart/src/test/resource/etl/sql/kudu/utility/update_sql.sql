UPDATE edumall_lake.gorders set is_etl = true where is_etl = false;
UPDATE edumall_lake.ad_bydates set is_etl = true where is_etl = false;
UPDATE edumall_lake.adaccounts set is_etl = true where is_etl = false;
UPDATE edumall_lake.ads set is_etl = true where is_etl = false;
UPDATE edumall_lake.bbuyers set is_etl = true where is_etl = false;
UPDATE edumall_lake.bcourses set is_etl = true where is_etl = false;
UPDATE edumall_lake.c3copies set is_etl = true where is_etl = false;
UPDATE edumall_lake.campaigns set is_etl = true where is_etl = false;
UPDATE edumall_lake.channel_course_users set is_etl = true where is_etl = false;
UPDATE edumall_lake.channel_courses set is_etl = true where is_etl = false;
UPDATE edumall_lake.channels set is_etl = true where is_etl = false;
UPDATE edumall_lake.contactc3s set is_etl = true where is_etl = false;
UPDATE edumall_lake.course_rates set is_etl = true where is_etl = false;
UPDATE edumall_lake.edumall_payments set is_etl = true where is_etl = false;
UPDATE edumall_lake.edumall_user set is_etl = true where is_etl = false;
UPDATE edumall_lake.eros_dms set is_etl = true where is_etl = false;
UPDATE edumall_lake.ad_groups set is_etl = true where is_etl = false;
UPDATE edumall_lake.group_works set is_etl = true where is_etl = false;
UPDATE edumall_lake.gusers set is_etl = true where is_etl = false;
UPDATE edumall_lake.history_orders set is_etl = true where is_etl = false;
UPDATE edumall_lake.landingpage set is_etl = true where is_etl = false;
UPDATE edumall_lake.mar_courses set is_etl = true where is_etl = false;
UPDATE edumall_lake.mar_departments set is_etl = true where is_etl = false;
UPDATE edumall_lake.mar_dms set is_etl = true where is_etl = false;
UPDATE edumall_lake.mar_teams_marketers set is_etl = true where is_etl = false;
UPDATE edumall_lake.marketers set is_etl = true where is_etl = false;
UPDATE edumall_lake.morders set is_etl = true where is_etl = false;
UPDATE edumall_lake.parcel_partners set is_etl = true where is_etl = false;
UPDATE edumall_lake.telesellers set is_etl = true where is_etl = false;
UPDATE edumall_lake.tmp_join_ad_bydates set is_etl = true where is_etl = false;
UPDATE edumall_lake.tmp_transactions_ranked set is_etl = true where is_etl = false;
UPDATE edumall_lake.transactions set is_etl = true where is_etl = false;



UPDATE edumall_lake.gorders set is_etl = false where _id in (Select _id from edumall_lake.gorders limit 10000);
UPDATE edumall_lake.ad_bydates set is_etl = false where id in (Select id from edumall_lake.ad_bydates limit 10000);

UPDATE edumall_lake.bbuyers set is_etl = false where id in (Select id from edumall_lake.bbuyers limit 10000);
UPDATE edumall_lake.bcourses set is_etl = false where id in (Select id from edumall_lake.bcourses limit 10000);
UPDATE edumall_lake.c3copies set is_etl = false where id in (Select id from edumall_lake.c3copies limit 10000);



UPDATE edumall_lake.contactc3s set is_etl = false where _id in (Select _id from edumall_lake.contactc3s limit 10000);
UPDATE edumall_lake.edumall_payments set is_etl = false where _id in (Select _id from edumall_lake.edumall_payments limit 10000);
UPDATE edumall_lake.edumall_user set is_etl = false where _id in (Select _id from edumall_lake.edumall_user limit 10000);
UPDATE edumall_lake.eros_dms set is_etl = false where _id in (Select _id from edumall_lake.eros_dms limit 10000);
UPDATE edumall_lake.history_orders set is_etl = false where _id in (Select _id from edumall_lake.history_orders limit 10000);

UPDATE edumall_lake.mar_dms set is_etl = false where _id in (Select _id from edumall_lake.mar_dms limit 10000);
UPDATE edumall_lake.morders set is_etl = false where _id in (Select _id from edumall_lake.morders limit 10000);


UPDATE edumall_lake.transactions set is_etl = false where id in (Select id from edumall_lake.transactions limit 10000);

UPDATE edumall_lake.group_works set is_etl = false where _id in (Select _id from edumall_lake.group_works limit 1);
UPDATE edumall_lake.parcel_partners set is_etl = false where _id in (Select _id from edumall_lake.parcel_partners limit 1);
UPDATE edumall_lake.gusers set is_etl = false where _id in (Select _id from edumall_lake.gusers limit 6);
UPDATE edumall_lake.mar_teams_marketers set is_etl = false where id in (Select id from edumall_lake.mar_teams_marketers limit 2);
UPDATE edumall_lake.channels set is_etl = false where id in (Select id from edumall_lake.channels limit 1);
UPDATE edumall_lake.adaccounts set is_etl = false where id in (Select id from edumall_lake.adaccounts limit 1);
UPDATE edumall_lake.course_rates set is_etl = false where id in (Select id from edumall_lake.course_rates limit 20);
UPDATE edumall_lake.mar_courses set is_etl = false where id in (Select id from edumall_lake.mar_courses limit 14);
UPDATE edumall_lake.mar_departments set is_etl = false where id in (Select id from edumall_lake.mar_departments limit 3);
UPDATE edumall_lake.marketers set is_etl = false where id in (Select id from edumall_lake.marketers limit 3);
UPDATE edumall_lake.channel_course_users set is_etl = false where id in (Select id from edumall_lake.channel_course_users limit 68);
UPDATE edumall_lake.ads set is_etl = false where id in (Select id from edumall_lake.ads limit 1000);
UPDATE edumall_lake.campaigns set is_etl = false where id in (Select id from edumall_lake.campaigns limit 200);
UPDATE edumall_lake.ad_groups set is_etl = false where id in (Select id from edumall_lake.ad_groups limit 580);
UPDATE edumall_lake.telesellers set is_etl = false where _id in (Select _id from edumall_lake.telesellers limit 10);
UPDATE edumall_lake.landingpage set is_etl = false where _id in (Select _id from edumall_lake.landingpage limit 40);
UPDATE edumall_lake.tmp_join_ad_bydates set is_etl = false where id in (Select id from edumall_lake.tmp_join_ad_bydates limit 1000);
UPDATE edumall_lake.channel_courses set is_etl = false where id in (Select id from edumall_lake.channel_courses limit 68);