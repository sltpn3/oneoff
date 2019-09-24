import psycopg2
import operator
import psycopg2.extras
from ConfigParser import ConfigParser
from datetime import datetime, timedelta
from pprint import pprint
import argparse

# ANTICIPATION;JOY;SADNESS;DISGUST;ANGER;SURPRISE;FEAR;TRUST


def emotion(candidate_id):
    citus_conn = psycopg2.connect(database=config.get("database_citus", "dbname"),
                                  user=config.get("database_citus", "dbuser"),
                                  password=config.get("database_citus", "dbpwd"),
                                  host=config.get("database_citus", "dbhost"),
                                  port=int(config.get("database_citus", "dbport"))
                                  )
    anticipation = 0
    joy = 0
    sadness = 0
    disgust = 0
    anger = 0
    surprise = 0
    fear = 0
    trust = 0

    cursor = citus_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    date_end = datetime.now()
    date_start = date_end - timedelta(minutes=10)

    if candidate_id:
        sql = """SELECT emotions, COUNT(emotions) FROM "pilpres_candidate_tweet_mapping"
                 WHERE created_at BETWEEN '{0}:01' AND '{1}:00' AND candidate_id = '{2}'
                 GROUP BY emotions""".format(date_start.strftime('%Y-%m-%d %H:%M'),
                                             date_end.strftime('%Y-%m-%d %H:%M'),
                                             candidate_id)
    else:
        sql = """SELECT emotions, COUNT(emotions) FROM "pilpres_candidate_tweet_mapping"
                 WHERE created_at BETWEEN '{0}:01' AND '{1}:00'
                 GROUP BY emotions""".format(date_start.strftime('%Y-%m-%d %H:%M'),
                                             date_end.strftime('%Y-%m-%d %H:%M'))
    # print sql
    cursor.execute(sql)
    rows = cursor.fetchall()
    # print rows
    cursor.close()
    citus_conn.close()
    for row in rows:
        if row[0][0] == 'anticipation':
            anticipation = row[1]
        elif row[0][0] == 'joy':
            joy = row[1]
        elif row[0][0] == 'sadness':
            sadness = row[1]
        elif row[0][0] == 'disgust':
            disgust = row[1]
        elif row[0][0] == 'anger':
            anger = row[1]
        elif row[0][0] == 'surprise':
            surprise = row[1]
        elif row[0][0] == 'fear':
            fear = row[1]
        elif row[0][0] == 'trust':
            trust = row[1]

    print '{};{};{};{};{};{};{};{};{};{}'.format(date_end.strftime('%H:%M:00'),
                                                 0,
                                                 anticipation,
                                                 joy,
                                                 sadness,
                                                 disgust,
                                                 anger,
                                                 surprise,
                                                 fear,
                                                 trust)


def emotion_last_2h(candidate_id):
    citus_conn = psycopg2.connect(database=config.get("database_citus", "dbname"),
                                  user=config.get("database_citus", "dbuser"),
                                  password=config.get("database_citus", "dbpwd"),
                                  host=config.get("database_citus", "dbhost"),
                                  port=int(config.get("database_citus", "dbport"))
                                  )
    anticipation = 0
    joy = 0
    sadness = 0
    disgust = 0
    anger = 0
    surprise = 0
    fear = 0
    trust = 0

    cursor = citus_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    start = datetime.now()

    date_end = datetime.now()
    date_start = date_end - timedelta(minutes=10)

#     print start

    for i in reversed(range(120)):
        date_end = start - timedelta(minutes=i)
        date_start = date_end - timedelta(minutes=10)

        if candidate_id:
            sql = """SELECT emotions, COUNT(emotions) FROM "pilpres_candidate_tweet_mapping"
                     WHERE created_at BETWEEN '{0}:01' AND '{1}:00' AND candidate_id = '{2}'
                     GROUP BY emotions""".format(date_start.strftime('%Y-%m-%d %H:%M'),
                                                 date_end.strftime('%Y-%m-%d %H:%M'),
                                                 candidate_id)
        else:
            sql = """SELECT emotions, COUNT(emotions) FROM "pilpres_candidate_tweet_mapping"
                     WHERE created_at BETWEEN '{0}:01' AND '{1}:00'
                     GROUP BY emotions""".format(date_start.strftime('%Y-%m-%d %H:%M'),
                                                 date_end.strftime('%Y-%m-%d %H:%M'))
        # print sql
        cursor.execute(sql)
        rows = cursor.fetchall()
        # print rows
        
        for row in rows:
            if row[0][0] == 'anticipation':
                anticipation = row[1]
            elif row[0][0] == 'joy':
                joy = row[1]
            elif row[0][0] == 'sadness':
                sadness = row[1]
            elif row[0][0] == 'disgust':
                disgust = row[1]
            elif row[0][0] == 'anger':
                anger = row[1]
            elif row[0][0] == 'surprise':
                surprise = row[1]
            elif row[0][0] == 'fear':
                fear = row[1]
            elif row[0][0] == 'trust':
                trust = row[1]
    
        print '{};{};{};{};{};{};{};{};{};{}'.format(date_end.strftime('%H:%M:00'),
                                                     0,
                                                     anticipation,
                                                     joy,
                                                     sadness,
                                                     disgust,
                                                     anger,
                                                     surprise,
                                                     fear,
                                                     trust)
    cursor.close()
    citus_conn.close()


def sentiment(candidate_id):
    citus_conn = psycopg2.connect(database=config.get("database_citus", "dbname"),
                                  user=config.get("database_citus", "dbuser"),
                                  password=config.get("database_citus", "dbpwd"),
                                  host=config.get("database_citus", "dbhost"),
                                  port=int(config.get("database_citus", "dbport"))
                                  )
    positive = 0
    negative = 0
    neutral = 0

    cursor = citus_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    date_end = datetime.now()
    date_start = date_end - timedelta(minutes=10)

    if candidate_id:
        sql = """SELECT sentiment, COUNT(*) FROM "pilpres_candidate_tweet_mapping"
                 WHERE created_at BETWEEN '{0}:01' AND '{1}:00' AND candidate_id = '{2}'
                 GROUP BY sentiment""".format(date_start.strftime('%Y-%m-%d %H:%M'),
                                              date_end.strftime('%Y-%m-%d %H:%M'),
                                              candidate_id)
    else:
        sql = """SELECT sentiment, COUNT(*) FROM "pilpres_candidate_tweet_mapping"
                 WHERE created_at BETWEEN '{0}:01' AND '{1}:00'
                 GROUP BY sentiment""".format(date_start.strftime('%Y-%m-%d %H:%M'),
                                              date_end.strftime('%Y-%m-%d %H:%M'),)

    # print sql
    cursor.execute(sql)
    rows = cursor.fetchall()
    # print rows
    cursor.close()
    citus_conn.close()
    for row in rows:
        if int(row[0]) == 0:
            neutral = row[1]
        elif int(row[0]) == 1:
            positive = row[1]
        elif int(row[0]) == -1:
            negative = row[1]

    print '{};{};{};{};{}'.format(date_end.strftime('%H:%M:00'),
                                  0,
                                  positive,
                                  neutral,
                                  negative)


def total():
    citus_conn = psycopg2.connect(database=config.get("database_citus", "dbname"),
                                  user=config.get("database_citus", "dbuser"),
                                  password=config.get("database_citus", "dbpwd"),
                                  host=config.get("database_citus", "dbhost"),
                                  port=int(config.get("database_citus", "dbport"))
                                  )
    sql = """SELECT candidate_id, COUNT(candidate_id) FROM "pilpres_candidate_tweet_mapping"
         WHERE created_at BETWEEN '{0} 00:00:00' AND '{0} 23:59:59'
         GROUP BY candidate_id""".format(datetime.now().strftime('%Y-%m-%d'))

    cursor = citus_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    citus_conn.close()
    total = {'1': 0,
             '2': 0,
             '3': 0}

    for row in rows:
        total[str(row[0])] = row[1]

    total['3'] = total['1'] + total['2'] + total['3']

    print '{};{};{}'.format(total['1'], total['2'], total['3'])


def compare_hours(hours=1):
    citus_conn = psycopg2.connect(database=config.get("database_citus", "dbname"),
                                  user=config.get("database_citus", "dbuser"),
                                  password=config.get("database_citus", "dbpwd"),
                                  host=config.get("database_citus", "dbhost"),
                                  port=int(config.get("database_citus", "dbport"))
                                  )
    time_end = datetime.now()
    time_start = datetime.now() - timedelta(hours=int(hours))

    sql = """SELECT candidate_id, COUNT(candidate_id) FROM "pilpres_candidate_tweet_mapping"
         WHERE created_at BETWEEN '{0}' AND '{1}' AND candidate_id IN (1,2)
         GROUP BY candidate_id""".format(time_start.strftime('%Y-%m-%d %H:%M:%S'),
                                         time_end.strftime('%Y-%m-%d %H:%M:%S'))

    cursor = citus_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    citus_conn.close()
    tweets = {'1': 0,
             '2': 0}
#     print rows

    for row in rows:
        tweets[str(row[0])] = row[1]

    tweets_total = tweets['1'] + tweets['2']
#     print tweets_total
    print 'PASLON;TOTAL;PERSEN'
    for i in [1,2]:
        persen = float(tweets[str(i)])/tweets_total * 100
        print 'PASLON{};{};{}'.format(i, tweets[str(i)], int(round(persen)))


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                        description='List Generator')

    argparser.add_argument('-c', '--config', help='Config File', metavar='', default='config_pilpres.conf')
    argparser.add_argument('-m', '--mode', help='Mode', metavar='', default='news')
    argparser.add_argument('-cd', '--candidate', help='Candidate ID', metavar='', default=None)
    argparser.add_argument('-h', '--hours', help='Hours', metavar='', default='1')

    args = argparser.parse_args()

    config_file = args.config
    config = ConfigParser()
    config.read(config_file)
    mode = args.mode
    if mode == 'emotion':
        emotion(args.candidate)
    if mode == 'sentiment':
        sentiment(args.candidate)
    if mode == 'total':
        total()
    if mode == 'emotion_last_2h':
        emotion_last_2h(args.candidate)
    if mode == 'compare_hours':
        compare_hours(args.hours)

#     emotion_last_2h(None)
#     compare_hours(3)
