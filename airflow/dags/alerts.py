import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
from datetime import datetime, timedelta, date
import io
import telegram
from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'i.korchagin',
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2022, 7, 12), 
}

schedule_interval = '*/15 * * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':#,
                      'user':#, 
                      'password':#
                     }

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def korchagin_alerts():    
    
    metrics_message = ['message_users', 'message_count', 'message_ads', 'message_organic',
                'message_Android', 'message_iOS', 'message_female', 'message_male']

    metrics_feed = ['feed_users', 'views', 'likes', 'ctr', 'feed_ads',
                    'feed_organic', 'feed_Android', 'feed_iOS', 'feed_female', 'feed_male']

    def check_anomaly(df, metric, a=4, n=5):
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25'] # межквартильный размах
        df['up'] = df['q75'] + a*df['iqr'] # верхняя граница
        df['low'] = df['q25'] - a*df['iqr'] # нижняя граница

        # сглаживание верхних и нижних границ межквартильного размаха
        df['up'] = df['up'].rolling(n, center=True, min_periods = 1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods = 1).mean()
   
        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df 

    def send_alert(df, metric, msg):

        my_token = #
        bot = telegram.Bot(token=my_token) 
        chat_id = # 

        sns.set(rc={'figure.figsize': (16, 10)})
        plt.tight_layout()

        # отрисовка трёх линий
        ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
        ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
        ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')

        # делаем пореже подпись по оси Х
        for ind, label in enumerate(ax.get_xticklabels()):  
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

        ax.set(xlabel='Время') 
        ax.set(ylabel=metric) 

        ax.set_title('{}'.format(metric)) 
        ax.set(ylim=(0, None)) 

        
        plot_object = io.BytesIO()
        ax.figure.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '{0}.png'.format(metric)
        plt.close()

        # отправляем алерт
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object) 
        print(msg)# оставляем в логах отметку
        return

    @task
    def extract_feed():              
        q_feed = ''' 
        SELECT
            toStartOfFifteenMinutes(time) as ts, 
            toDate(ts) as date, 
            formatDateTime(ts, '%R') as hm, 
            uniqExact(user_id) as feed_users,
            countIf(user_id, action = 'view') as views,
            countIf(user_id, action = 'like') as likes,
            likes / views as ctr,
            countIf(user_id, source = 'ads') as feed_ads,
            countIf(user_id, source = 'organic') as feed_organic,
            countIf(user_id, os = 'Android') as feed_Android,
            countIf(user_id, os = 'iOS') as feed_iOS,
            countIf(user_id, gender = 1) as feed_female,
            countIf(user_id, gender = 0) as feed_male
        FROM simulator_20220620.feed_actions
        WHERE time >=  today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts 
        '''
        
        data = ph.read_clickhouse(query=q_feed, connection = connection)

        return data
    
    @task    
    def extract_message(): 
        
        q_message = ''' 
        SELECT
            toStartOfFifteenMinutes(time) as ts, 
            toDate(ts) as date, 
            formatDateTime(ts, '%R') as hm, 
            uniqExact(user_id) as message_users,
            count(user_id) as message_count,
            countIf(user_id, source = 'ads') as message_ads,
            countIf(user_id, source = 'organic') as message_organic,
            countIf(user_id, os = 'Android') as message_Android,
            countIf(user_id, os = 'iOS') as message_iOS,
            countIf(user_id, gender = 1) as message_female,
            countIf(user_id, gender = 0) as message_male
        FROM simulator_20220620.message_actions
        WHERE time >=  today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        '''

        data = ph.read_clickhouse(query=q_message, connection = connection)

        return data    


    @task
    def alert_anomaly(data, metrics_list): 
        
        for metric in metrics_list: 
            df = data[['ts','date','hm', metric]].copy()# создаем новый объект с копией данных 

            is_alert, df = check_anomaly(df, metric)        

            if is_alert == 1: # or True для теста
                msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {last_value_diff:.2%}'''\
                    .format(metric=metric,
                            current_value=df[metric].iloc[-1], 
                            last_value_diff=abs(1-df[metric].iloc[-1]/df[metric].iloc[-2]))
                send_alert(df, metric, msg)                                
            else:
                print(metric, ': no anomaly')# оставляем в логах отметку

        return

    df_feed = extract_feed()
    df_message = extract_message()

    alert_anomaly(df_feed, metrics_feed) 
    alert_anomaly(df_message, metrics_message)        
    
    
korchagin_alerts = korchagin_alerts()
