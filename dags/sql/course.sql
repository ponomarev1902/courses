 INSERT INTO dbo.lco_course(
    co_id,
    metadata_co,
    co_tit_course_title,
    co_ico_course_iconurl,
    co_del_course_deleted,
    co_cre_course_created,
    co_aut_aut_isautocourseenrole,
    co_dem_dem_isdemoenrole,
    co_upd_course_updatedat
 )
 SELECT 
    id,
    1,
    title,
    icon_url,
    deleted_at,
    created_at,
    is_auto_course_enroll,
    is_demo_enroll,
    updated_at
 FROM
    course_temp
ON CONFLICT DO NOTHING;