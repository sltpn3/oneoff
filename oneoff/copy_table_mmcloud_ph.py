import MySQLdb
from datetime import datetime, timedelta


def copy_person(start_date=None, _range=1):
    conn = MySQLdb.connect('192.168.99.177', 'backend', 'rahasia123', 'mm_prod_news')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'SELECT t_id FROM topic WHERE w_id=38'
    cursor.execute(sql)
    topics = cursor.fetchall()
    if start_date:
        start_date = datetime.strptime(start_date, '%Y%m%d')
    else:
        start_date = datetime.now()
    for topic in topics:
        for i in range(_range):
            _date = (start_date - timedelta(days=i)).strftime('%Y%m%d')
            sql = """INSERT IGNORE INTO person_stat2 
                     SELECT * FROM person_stat 
                     WHERE t_id = {}
                     AND d_day = '{}'""".format(topic['t_id'], _date)
            cursor.execute(sql)
            print '[{}] {}|{} Done'.format(datetime.now(), topic['t_id'], _date)
            conn.commit()

    cursor.close()
    conn.close()
    
def copy_organization(start_date=None, _range=1):
    conn = MySQLdb.connect('192.168.99.177', 'backend', 'rahasia123', 'mm_prod_news')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'SELECT t_id FROM topic WHERE w_id=38'
    cursor.execute(sql)
    topics = cursor.fetchall()
    if start_date:
        start_date = datetime.strptime(start_date, '%Y%m%d')
    else:
        start_date = datetime.now()
    for topic in topics:
        for i in range(_range):
            _date = (start_date - timedelta(days=i)).strftime('%Y%m%d')
            sql = """INSERT IGNORE INTO organization_stat2 
                     SELECT * FROM organization_stat 
                     WHERE t_id = {}
                     AND d_day = '{}'""".format(topic['t_id'], _date)
            cursor.execute(sql)
            print '[{}] {}|{} Done'.format(datetime.now(), topic['t_id'], _date)
            conn.commit()

    cursor.close()
    conn.close()
    
def copy_influencer(start_date=None, _range=1):
    conn = MySQLdb.connect('192.168.99.177', 'backend', 'rahasia123', 'mm_prod_news')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = 'SELECT t_id FROM topic WHERE w_id=38'
    cursor.execute(sql)
    topics = cursor.fetchall()
    if start_date:
        start_date = datetime.strptime(start_date, '%Y%m%d')
    else:
        start_date = datetime.now()
    for topic in topics:
        for i in range(_range):
            _date = (start_date - timedelta(days=i)).strftime('%Y%m%d')
            sql = """INSERT IGNORE INTO influencer_stat2 
                     SELECT * FROM influencer_stat 
                     WHERE t_id = {}
                     AND d_day = '{}'""".format(topic['t_id'], _date)
            cursor.execute(sql)
            print '[{}] {}|{} Done'.format(datetime.now(), topic['t_id'], _date)
            conn.commit()

    cursor.close()
    conn.close()


copy_person(_range=360)
copy_organization(_range=360)