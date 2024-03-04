INSERT INTO
    dbo.lmo_module
    (
        mo_id,
        metadata_mo,
        mo_upd_module_updatedat,
        mo_tit_module_title,
        mo_cre_module_createdat,
        mo_ord_module_orderinstream,
        mo_del_module_deletedat
    )
SELECT 
    id
    ,3
    ,updated_at
    ,title
    ,created_at
    ,order_in_stream
    ,deleted_at
FROM
    temp_module;

INSERT INTO
    dbo.lst_consists_mo_of
    (
        metadata_st_consists_mo_of
        ,st_consists_mo_of_changedat
        ,st_id_consists
        ,mo_id_of
    )
SELECT
    3
    ,NOW()
    ,stream_id
    ,id
FROM
    temp_module;