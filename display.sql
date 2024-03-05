SELECT 
	co.co_id  as id
	,co.co_tit_course_title as course_title
	,co.co_ico_course_iconurl as course_iconurl
	,co.co_del_course_deleted as course_deleted
	,co.co_cre_course_created as course_created
	,co.co_aut_aut_isautocourseenrole as isautocourseenrole
	,co.co_upd_course_updatedat as course_updatedat
	,st.st_id as stream_id
	,st.st_nam_stream_name as stream_name
	,st.st_del_stream_deletedat as stream_deletedat
	,st.st_sta_stream_startat as stream_startat
FROM dbo.nco_course co
LEFT JOIN
	dbo.lco_learned_st_on 
	ON co.co_id = lco_learned_st_on.co_id_learned
LEFT JOIN
	dbo.nst_stream st
	ON st.st_id = lco_learned_st_on.st_id_on
