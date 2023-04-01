""" Второе задание """

from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'ikorchagin',
    'poke_interval': 600,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '* * * * 1-6'
    }


@dag(default_args = DEFAULT_ARGS, catchup=True, tags=['ikorchagin'])
def ikorchagin_dag():

    #Получаем сегодняшний день недели
    @task()
    def day_now():
        weekday = datetime.now().weekday() + 1
        return weekday

    #Получаем данные из GreenPlum за соответствующий день
    @task()
    def get_data_from_gp(daynow):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {daynow}')
        db_result = cursor.fetchone()[0]
        #query_res = cursor.fetchall()
        return db_result
    #Выводим на печать в лог
    @task()
    def print_data_to_log(total):
        this_day = datetime.now()
        logging.info(f'Now is {this_day}')
        logging.info(total)

    print_data_to_log(get_data_from_gp(day_now()))

ikorchagin_dag = ikorchagin_dag()

