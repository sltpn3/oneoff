import MySQLdb
from datetime import datetime, timedelta

sql = """SELECT t_id FROM topic WHERE t_id > 0 AND t_status=0 ORDER BY last_access DESC"""

conn = MySQLdb.connect('192.168.150.154', 'backend', 'rahasia123', 'mm_prod_news')
cursor = conn.cursor(MySQLdb.cursors.DictCursor)
cursor.execute(sql)
rows = cursor.fetchall()

cursor.close()
conn.close()

today = datetime.now() - timedelta(days=1)
range_day = 180

old_table = 'influencer_stat_old'
new_table = 'influencer_stat'

for n in range(range_day):
    for row in rows:
        try:
            _day = (today - timedelta(days=n)).strftime('%Y%m%d')
            sql = """INSERT IGNORE INTO {} SELECT * FROM {} WHERE t_id = {} AND d_day={}""".format(new_table, old_table,
                                                                                                   row['t_id'], _day)
            print '[{}] {}'.format(datetime.now(), sql)
            conn = MySQLdb.connect('192.168.150.154', 'backend', 'rahasia123', 'mm_prod_news')
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
            print '[{}] {} Done'.format(datetime.now(), sql)
        except:
            pass