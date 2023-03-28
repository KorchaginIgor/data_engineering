import requests
import logging
import pandas as pd
from airflow.models import BaseOperator




class I_Korchagin_8_Ram_Top_Location(BaseOperator):
    """
    Operator for get top 3 RAM location by residents count
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    # функция для подсчета числа страниц с локациями
    def get_page_count(self, url: str) -> int:
        r = requests.get(url)
        page_count = r.json().get('info').get('pages')
        return int(page_count)

    # функция для вычисления топ-3 локаций по кол-ву резидентов
    def execute(self, context):
        api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        df = pd.DataFrame()
        for page in range(self.get_page_count(api_url.format(pg='1'))):
            r = requests.get(api_url.format(pg=str(page + 1))).json().get('results')
            for i in range(len(r)):
                df = df.append(r[i], ignore_index=True)
        df['resident_cnt'] = df.residents.apply(lambda x: len(x))
        df = df.sort_values('resident_cnt', ascending=False).head(3)
        df = df.drop(['created', 'url', 'residents'], 1)
        tuples = [tuple(x) for x in df.to_numpy()]
        return ','.join(map(str, tuples))