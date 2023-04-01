import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':#,
                      'user':#, 
                      'password':#
                     }
default_args = {
    'owner': 'i.korchagin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 5)
    }

schedule_interval = '0 11 * * *'

chat_id = #
my_token = # 
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def korchagin_tlg_report_dag():
    @task()
    def extract():
        query = '''
        SELECT toDate(time) AS day,
            count(DISTINCT user_id) AS DAU,
            countIf(user_id, action='view') as views,
            countIf(user_id, action='like') as likes,
            round(countIf(user_id, action='like') / countIf(user_id, action='view'), 3) AS CTR
        FROM simulator_20220620.feed_actions
        WHERE toDate(time) >= today()-7 and toDate(time) < today()
        GROUP BY toDate(time)
        ORDER BY day
        '''

        df = ph.read_clickhouse(query, connection=connection)
        
        return df
    
    @task
    def send_report(df):
        yesterday = df['day'].max().strftime('%d %B %Y')
        df['day'] = df['day'].dt.strftime("%b-%d")
        ctr = df.loc[6,'CTR']
        likes = df.loc[6,'likes']
        DAU = df.loc[6,'DAU']
        views = df.loc[6,'views']
        msg = f'Отчет по ленте за {yesterday}\nDAU: {DAU}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr}\n\nПодробнее тут:https://superset.lab.karpov.courses/superset/dashboard/1116/'

        bot.sendMessage(chat_id=chat_id,text=msg)


        list_metric = ['DAU', 'views', 'likes', 'CTR']
        list_title = ['DAU ленты за прошедшие 7 дней', 'Просмотры ленты за прошедшие 7 дней', 
                      'Лайки ленты за прошедшие 7 дней', 'CTR ленты за прошедшие 7 дней']
        features = pd.DataFrame({'metric': list_metric, 'title': list_title})

        def print_photo(df, title, metric):
            sns.set_style("whitegrid")
            sns.lineplot(x='day',  y=metric, data=df)
            plt.title(title)
            plt.xlabel("День")
            plt.ylabel("")
            plt.xticks(rotation=360)

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{metric}.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)


        for index, row in features.iterrows():
            print_photo(df, row[1], row[0])    


    df = extract()   
    send_report(df)

korchagin_tlg_report_dag = korchagin_tlg_report_dag()
