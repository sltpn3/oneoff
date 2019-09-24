import json
import MySQLdb


def process():
    result = {}
    with open('data_solr_facebook.txt', 'r') as f:
        d = f.read()
    data = json.loads(d)
    for post in data:
        if post['user_id'] not in result:
            result[post['user_id']] = {'0': 0,
                                       '1': 0,
                                       '-1': 0}
        result[post['user_id']][str(post['sentiment'])] += 1

    for i in result:
        result[i]['total'] = result[i]['0'] + result[i]['1'] + result[i]['-1']
        print '{}|{}|{}|{}|{}'.format(i,
                                      result[i]['-1'],
                                      result[i]['0'],
                                      result[i]['1'],
                                      result[i]['total'])


def process2():
    with open('fb_top100.csv', 'r') as f:
        users = f.readlines()
    conn = MySQLdb.connect('192.168.150.158', 'backend', 'rahasia123', 'ipa_facebook')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    users = [x.replace('\n', '').lower() for x in users]
    for user in users:
        _id, neg, neu, pos, total = user.split(',')
        sql = 'SELECT * FROM fb_user WHERE user_id="{}"'.format(_id)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
            print '{};{};{};{};{};{};{};{}'.format(_id, row['screen_name'], row['name'], row['gender'],
                                          neg, neu, pos, total)
        
    cursor.close()
    conn.close()

process2()
