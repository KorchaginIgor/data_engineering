""" Берем данные из rickandmortyapi.com,
    находим топ3 локации по числу резидентов
    и помещаем их в таблицу в GreenPlum
"""
import requests
from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from i_korchagin_8_plugins.i_korchagin_8_operator import I_Korchagin_8_Ram_Top_Location

DEFAULT_ARGS = {'start_date': datetime(2022, 9, 4),
                'owner': 'i-korchagin',
                'poke_interval': 600,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'schedule_interval': '* 10 * * *',
                'catchup': False
                }

api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

with DAG('ikorchagin_ram_location',
         default_args=DEFAULT_ARGS,
         tags=['ikorchagin'],
         ) as dag:
    start = DummyOperator(task_id='start', dag=dag)

    end = DummyOperator(task_id='end', dag=dag)

    top_3_location = I_Korchagin_8_Ram_Top_Location(task_id='top_3_location',
                                                  dag=dag)

    load_data_to_gp = PostgresOperator(task_id='load_data_to_gp',
                                       dag=dag,
                                       postgres_conn_id='conn_greenplum_write',
                                       sql=['TRUNCATE TABLE i_korchagin_8_ram_location',
                                            'INSERT INTO public.i_korchagin_8_ram_location '
                                            'VALUES {{ ti.xcom_pull(task_ids = "top_3_location") }}']
                                       )

    start >> top_3_location >> load_data_to_gp >> end