    
INSERT INTO 
    dbo.lst_stream(    
        st_id,
        metadata_st,
        st_del_stream_deletedat,
        st_cre_stream_createdat,
        st_ena_stream_endat,
        st_sta_stream_startat,
        st_hom_stream_homeworkdeadlinedays,
        st_ope_ope_isopen,
        st_nam_stream_name,
        st_upd_stream_updatedat
    )
SELECT 
    id
    ,2
    ,deleted_at
    ,created_at
    ,end_at
    ,start_at
    ,homework_deadline_days
    ,is_open
    ,name
    ,updated_at
FROM
    temp_stream;

INSERT INTO 
    dbo.lco_learned_st_on(
        co_learned_st_on_changedat
        ,co_id_learned
        ,st_id_on
        ,metadata_co_learned_st_on
    )
SELECT 
    NOW()
    ,course_id
    ,id
    ,2 
FROM 
    temp_stream;

