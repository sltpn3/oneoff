from library.eBeanstalk import Pusher
import MySQLdb

_dates = ['2019-06-28',
          '2019-06-29',
          '2019-06-30',
          '2019-07-01',
          '2019-07-02',
          '2019-07-03',
          '2019-07-04']

langs = ['en', 'id', 'my']


pusher = Pusher('malaysia_db', '192.168.99.220')

for _date in _dates:
    for lang in langs:
        tablename = '{}_{}_{}'.format(_date[:4], _date[5:7], lang)
        sql = """SELECT news_id FROM {} WHERE DATE(news_date) = '{}'""".format(tablename, _date)
        conn = MySQLdb.connect('192.168.99.177', 'backend', 'rahasia123', 'online_news')
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        for row in rows:
            job = '{}|{}'.format(row['news_id'], tablename)
            pusher.setJob('job', priority=7, ttr=3600)
            print job
