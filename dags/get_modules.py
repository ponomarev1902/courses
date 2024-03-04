import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="process-module",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessModule():
    create_module_temp_table = PostgresOperator(
        task_id="create_module_temp_data",
        postgres_conn_id="internal-db",
        sql="sql/temp_module.sql"
    )

    @task
    def get_data():
        from psycopg2.extras import execute_batch
        with(
            PostgresHook(postgres_conn_id="external-db").get_conn() as ext_conn,
            PostgresHook(postgres_conn_id="internal-db").get_conn() as conn
        ):
            with (
                ext_conn.cursor() as ext_cur,
                conn.cursor() as cur
            ):
                ext_cur.execute("SELECT * FROM stream_module;")
                cur = conn.cursor()
                execute_batch(
                    cur,
                    "INSERT INTO temp_module VALUES(%s, %s, %s, %s, %s, %s, %s)",
                    ext_cur.fetchall()
                )
                conn.commit()

    insert_module = PostgresOperator(
        task_id="insert_module",
        postgres_conn_id="internal-db",
        sql="sql/module.sql"
    )

    create_module_temp_table  >> get_data() >> insert_module

dag = ProcessModule()