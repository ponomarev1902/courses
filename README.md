# courses
Проект содержит файл docker-compose.yaml, взятый с сайта https://airflow.apache.org/ и отредактированный. Я добавил туда две базы PostgreSQL (онда имитирует внешний источник данных, другая внутренния в DWH) и pgAdmin для работы с ними.
1. В качестве модели данных была выбрана Anchor, так как она позволяет вносить изменения в структуру данных (можно предположить что в существующую модель данных будет добавлена сущность Учитель) так и поддерживать историю изменения данных в источнике. Учитывая поля update_at, можно предположить, что в данные регулярно вносятся изменения.
2. ETL процесс реализован с помощью Airflow. Директория data содержит тестовые данные и сгенерированный файл с помощью anchormodeling.com
3. Мне не хватило времени сделать полноценную ветрину данных, есть только пример ее части
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
