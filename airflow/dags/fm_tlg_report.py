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
chat_id =  #
my_token = # 
bot = telegram.Bot(token=my_token)

connection_my = {'host': 'https://clickhouse.lab.karpov.courses',
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

query_only_feed_yesterday = """
    SELECT round(count(DISTINCT user_id)/
      (SELECT count(DISTINCT user_id)
       FROM simulator_20220620.feed_actions
       WHERE toDate(time) >= today() - 1
       AND toDate(time) < today()), 2) * 100  AS share 
    from simulator_20220620.feed_actions 
    where user_id not IN (select DISTINCT user_id from simulator_20220620.message_actions) 
    and (toDate(time) >= today() - 1 AND toDate(time) < today())
    """

query_avg_views_per_user_yesterday = """
    SELECT round(countIf(action='view')/count(DISTINCT user_id), 1) AS avg_view_per_user
    FROM simulator_20220620.feed_actions
    WHERE toDate(time) = today() - 1
    """

query_avg_likes_per_user_yesterday = """
    SELECT round(countIf(action='like')/count(DISTINCT user_id), 1) AS avg_like_per_user
    FROM simulator_20220620.feed_actions
    WHERE toDate(time) = today() - 1
    """

query_avg_send_messages_per_user_yesterday = """
    SELECT round(count(user_id)/count(DISTINCT user_id), 1) AS avg_send_mes_per_user
    FROM simulator_20220620.message_actions
    WHERE toDate(time) = today() - 1
    """

query_new_users_7days = """
    SELECT event_date, count(DISTINCT user_id) AS cnt_users
    FROM
      (SELECT user_id, min(toDate(time)) AS event_date
       FROM simulator_20220620.feed_actions
       GROUP BY user_id
       UNION ALL
       SELECT user_id, min(toDate(time)) AS event_date
       FROM simulator_20220620.message_actions
       GROUP BY user_id) AS t1
    WHERE  event_date >= today() - 7
    AND event_date < today()
    GROUP BY event_date
    """

query_dau_app_last_7days = """
    SELECT event_date, count(DISTINCT user_id) AS cnt_users
    FROM
      (SELECT user_id, toDate(time) AS event_date
       FROM simulator_20220620.feed_actions
       WHERE  event_date >= today() - 7
       AND event_date < today()
       UNION ALL
       SELECT user_id, toDate(time) AS event_date
       FROM simulator_20220620.message_actions
       WHERE  event_date >= today() - 7
       AND event_date < today()) AS t1
    GROUP BY event_date
    """

query_users_use_feed_and_messanger_last_7days = """
    SELECT count(DISTINCT t1.user_id) AS cnt_users, t1.event_date
    FROM       
      (SELECT user_id, toDate(time) AS event_date
       FROM simulator_20220620.message_actions
       WHERE toDate(time) <= today() - 1
       AND toDate(time) >= today() - 7) AS t1
    INNER JOIN
      (SELECT user_id, toDate(time) AS event_date
       FROM simulator_20220620.feed_actions
       WHERE toDate(time) <= today() - 1
       AND toDate(time) >= today() - 7) AS t2 
    ON t1.user_id = t2.user_id
    GROUP BY t1.event_date
    """

query_top_1000_post_yesterday = """
    SELECT post_id AS post_id,
          countIf(action='view') AS views,
          countIf(action='like') AS likes,
          round(countIf(action='like') / countIf(action='view'), 3) AS ctr
    FROM simulator_20220620.feed_actions
    WHERE toDate(time) >= today() - 1
    AND toDate(time) < today()
    GROUP BY post_id
    ORDER BY views DESC
    LIMIT 1000
    """

query_view_like_android = """                    
    select event_date, views, likes
    from test.ikorchagin
    where metric_value = 'Android' and (event_date between today() - 7 and today() -1)
    order BY event_date
    """ 

query_view_like_ios = """                    
    select event_date, views, likes
    from test.ikorchagin
    where metric_value = 'iOS' and (event_date between today() - 7 and today() -1)
    order BY event_date
    """  

query_message_android = """                    
    select event_date, messages_received, messages_sent, users_received, users_sent
    from test.ikorchagin
    where metric_value = 'Android' and (event_date between today() - 7 and today() -1)
    order BY event_date
    """  

query_message_ios = """                    
    select event_date, messages_received, messages_sent, users_received, users_sent
    from test.ikorchagin
    where metric_value = 'iOS' and (event_date between today() - 7 and today() -1)
    order BY event_date
    """ 

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def korchagin_tlg_report_fm_dag():
    
    @task
    def only_feed_yesterday():
    
        df_only_feed_yesterday = ph.read_clickhouse(query_only_feed_yesterday, connection=connection)
        return df_only_feed_yesterday.share[0]
    
    @task
    def avg_views_per_user_yesterday():
    
        df_views_per_user_yesterday = ph.read_clickhouse(query_avg_views_per_user_yesterday, connection=connection)
        return df_views_per_user_yesterday.avg_view_per_user[0]
    
    @task
    def avg_likes_per_user_yesterday():
    
        df_likes_per_user_yesterday = ph.read_clickhouse(query_avg_likes_per_user_yesterday, connection=connection)
        return df_likes_per_user_yesterday.avg_like_per_user[0]
    
    @task
    def avg_send_messages_per_user_yesterday():
    
        df_send_messages_per_user_yesterday = ph.read_clickhouse(query_avg_send_messages_per_user_yesterday, connection=connection)
        return df_send_messages_per_user_yesterday.avg_send_mes_per_user[0]
    
    @task
    def new_users_7days():
    
        df_new_users = ph.read_clickhouse(query_new_users_7days, connection=connection)
        df_new_users['event_date'] = df_new_users['event_date'].dt.strftime("%b-%d")
        return df_new_users 
    
    @task
    def dau_app_last_7days():
    
        df_dau_app = ph.read_clickhouse(query_dau_app_last_7days, connection=connection)
        df_dau_app['event_date'] = df_dau_app['event_date'].dt.strftime("%b-%d")
        return df_dau_app 

    @task
    def users_use_feed_and_messanger_last_7days():
    
        df_use_fm = ph.read_clickhouse(query_users_use_feed_and_messanger_last_7days, connection=connection)
        df_use_fm['event_date'] = df_use_fm['event_date'].dt.strftime("%b-%d")
        return df_use_fm
    
    @task
    def top_1000_post_yesterday():
    
        top_1000_post = ph.read_clickhouse(query_top_1000_post_yesterday, connection=connection)
        return top_1000_post 

    @task
    def users_view_like_android_last_7days():
    
        df_vl_android = ph.read_clickhouse(query_view_like_android, connection=connection_my)
        df_vl_android['event_date'] = df_vl_android['event_date'].dt.strftime("%b-%d")
        return df_vl_android 

    @task
    def users_view_like_ios_last_7days():
    
        df_vl_ios = ph.read_clickhouse(query_view_like_ios, connection=connection_my)
        df_vl_ios['event_date'] = df_vl_ios['event_date'].dt.strftime("%b-%d")
        return df_vl_ios
    
    @task
    def users_messanger_android_last_7days():
    
        df_m_android = ph.read_clickhouse(query_message_android, connection=connection_my)
        df_m_android['event_date'] = df_m_android['event_date'].dt.strftime("%b-%d")
        return df_m_android
    
    @task
    def users_messanger_ios_last_7days():

        df_m_ios = ph.read_clickhouse(query_message_ios, connection=connection_my)
        df_m_ios['event_date'] = df_m_ios['event_date'].dt.strftime("%b-%d")
        return df_m_ios
    
    @task
    def send_report(df_only_feed_yesterday, df_views_per_user_yesterday, df_likes_per_user_yesterday,
                    df_send_messages_per_user_yesterday, df_new_users, df_dau_app, df_use_fm,
                    top_1000_post, df_vl_android, df_vl_ios, df_m_android, df_m_ios):
        
        yesterday = (datetime.today() - pd.Timedelta(days=1)).strftime('%d %B %Y')
        DAU = df_dau_app.loc[6,'cnt_users']
        new_users = df_new_users.loc[6,'cnt_users']
        ios_m_r = df_m_ios.loc[6,'messages_received']
        android_m_r = df_m_android.loc[6,'messages_received']
        ios_m_s = df_m_ios.loc[6,'messages_sent']
        android_m_s = df_m_android.loc[6,'messages_sent']
        ios_u_r = df_m_ios.loc[6,'users_received']
        android_u_r = df_m_android.loc[6,'users_received']
        ios_u_s = df_m_ios.loc[6,'users_sent']
        android_u_s = df_m_android.loc[6,'users_sent']
        ios_v = df_vl_ios.loc[6,'views']
        android_v = df_vl_android.loc[6,'views']
        ios_l = df_vl_ios.loc[6,'likes']
        android_l = df_vl_android.loc[6,'likes']

        msg = 'Отчет по приложению за {yesterday}\nСреднее кол-во просмотров на пользвателя: {df_views_per_user_yesterday}'\
        '\nСреднее кол-во лайков на пользвателя: {df_likes_per_user_yesterday}'\
        '\nСреднее кол-во отправленных сообщений на пользвателя: {df_send_messages_per_user_yesterday}'\
        '\nНовые пользователи: {new_users}'\
        '\nDAU: {DAU}'\
        '\nПользовались только лентой: {df_only_feed_yesterday}%'\
        '\n\niOS|Android:\nКоличество полученных сообщений: {ios_m_r}|{android_m_r}'\
        '\nКоличество отправленных сообщений: {ios_m_s}|{android_m_s}'\
        '\nОт скольких пользователей получили сообщения: {ios_u_r}|{android_u_r}'\
        '\nСкольким пользователям отправили сообщение: {ios_u_s}|{android_u_s}'\
        '\nКоличество просмотров: {ios_v}|{android_v}'\
        '\nКоличество лайков: {ios_l}|{android_l}'\
        '\n\nТоп 1000 просматриваемых постов:'\
        .format(yesterday=yesterday,df_views_per_user_yesterday=df_views_per_user_yesterday,
                df_likes_per_user_yesterday=df_likes_per_user_yesterday,new_users=new_users,DAU=DAU,
                df_only_feed_yesterday=df_only_feed_yesterday,ios_m_r=ios_m_r,
                ios_m_s=ios_m_s,android_m_r=android_m_r,android_m_s=android_m_s, ios_u_r=ios_u_r,
                ios_u_s=ios_u_s,android_u_r=android_u_r,android_u_s=android_u_s, ios_v=ios_v,android_v=android_v,
                ios_l= ios_l, android_l=android_l, df_send_messages_per_user_yesterday= df_send_messages_per_user_yesterday)
    #ф-строка не работала пришлось так выкрутиться(не очень оптимально)

        bot.sendMessage(chat_id=chat_id,text=msg)

        file_object = io.StringIO()
        top_1000_post.to_csv(file_object, index=False)
        file_object.name = 'top 1000 post yesterday.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)


        
        list_title = ['Новые пользователи за прошедшие 7 дней','DAU за прошедшие 7 дней',
                      'Пользователи, которые использовали ленту\nи мессенджер в один день за прошедшие 7 дней']
        list_df = [df_new_users, df_dau_app, df_use_fm]
        features = pd.DataFrame({'title': list_title, 'df': list_df})

        def print_photo(df, title):
            sns.set_style("whitegrid")
            sns.lineplot(x='event_date',  y='cnt_users', data=df)
            plt.title(title)
            plt.xlabel("День")
            plt.ylabel("")
            plt.xticks(rotation=360)

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{df}.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)


        for index, row in features.iterrows():
            print_photo(row[1], row[0])

        list_metric = ['views','likes', 'messages_received',  'messages_sent', 'users_received', 'users_sent']
        list_title = ['Количество просмотров на разных OS за прошедшие 7 дней',
                      'Количество лайков на разных OS за прошедшие 7 дней',
                      'Количество полученных сообщений на разных OS за прошедшие 7 дней',
                      'Количество отправленных сообщений на разных OS за прошедшие 7 дней',
                      'От скольких пользователей получили \nсообщения на разных OS за прошедшие 7 дней',
                      'Скольким пользователям отправили \nсообщение на разных OS за прошедшие 7 дней']
        list_df_1 = [df_vl_ios, df_vl_ios, df_m_ios, df_m_ios, df_m_ios, df_m_ios]
        list_df_2 = [df_vl_android, df_vl_android, df_m_android, df_m_android, df_m_android, df_m_android]
        features = pd.DataFrame({'metric': list_metric, 'title': list_title, 'df_ios': list_df_1, 'df_android': list_df_2})

        def print_photo_os(metric, title, df_ios, df_android):
            sns.set_style("whitegrid")
            sns.lineplot(x='event_date', y=metric, data=df_ios)
            sns.lineplot(x='event_date', y=metric, data=df_android)
            plt.title(title)
            plt.legend(['iOS', 'Android'])
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
            print_photo_os(row[0], row[1], row[2], row[3])

        
    df_only_feed_yesterday = only_feed_yesterday()
    df_views_per_user_yesterday = avg_views_per_user_yesterday()
    df_likes_per_user_yesterday = avg_likes_per_user_yesterday()
    df_send_messages_per_user_yesterday = avg_send_messages_per_user_yesterday()
    df_new_users = new_users_7days()
    df_dau_app = dau_app_last_7days()
    df_use_fm = users_use_feed_and_messanger_last_7days()
    top_1000_post = top_1000_post_yesterday()
    df_vl_android = users_view_like_android_last_7days()
    df_vl_ios = users_view_like_ios_last_7days()
    df_m_android = users_messanger_android_last_7days()
    df_m_ios = users_messanger_ios_last_7days()
    send_report(df_only_feed_yesterday, df_views_per_user_yesterday, df_likes_per_user_yesterday,
                df_send_messages_per_user_yesterday, df_new_users, df_dau_app, df_use_fm,
                top_1000_post, df_vl_android, df_vl_ios, df_m_android, df_m_ios)

korchagin_tlg_report_fm_dag = korchagin_tlg_report_fm_dag()
