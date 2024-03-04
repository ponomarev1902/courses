BEGIN;
INSERT INTO
    dbo.lLE_Lesson
    (
        le_id,
        metadata_le,
        le_del_lesson_deletedat,
        le_joi_lesson_onlinelessonjoinurl,
        le_rec_lesson_onlinelessonrecordingurl,
        le_tit_lesson_title,
        le_edt_lesson_endat,
        le_hom_lesson_homeworkurl,
        le_des_lesson_description,
        le_sta_lesson_startat
    )
SELECT
    id
    ,4
    ,deleted_at
    ,online_lesson_join_url
    ,online_lesson_recording_url
    ,title
    ,end_at
    ,homework_url
    ,description
    ,start_at
FROM
    temp_lesson;

INSERT INTO
    dbo.TE_Teacher
    (
        te_id
        ,metadata_te
    )
SELECT
    teacher_id
    ,4
FROM
    temp_lesson
ON CONFLICT DO NOTHING;

INSERT INTO
    dbo.lLE_tought_TE_by
    (
        metadata_le_tought_te_by
        ,le_tought_te_by_changedat
        ,le_id_tought
        ,te_id_by
    )
SELECT 
    4
    ,NOW()
    ,id
    ,teacher_id
FROM
    temp_lesson;
COMMIT;