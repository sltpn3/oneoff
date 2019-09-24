from library.eBsolr import eBsolr
import json
import MySQLdb


def save_data():
    solr = eBsolr('http://192.168.150.101:8983/solr/instagram', 'config.conf')

    query = 'polri AND pub_year:2019'

    run = True
    data = []
    i = 0
    while run:
        print i
        response = solr.getDocs(query, 'user_id, sentiment', start=i * 10000, rows=10000)
        i += 1
        data += response['docs']
        if len(response['docs']) < 10000:
            run = False

    f = open('data_solr_instagram.txt', 'w')
    f.write(json.dumps(data))
    f.close()
    print 'end'


def get_user():
    with open('data_user_polri.txt', 'r') as f:
        users = f.readlines()

    users = [x.replace('\n', '').lower() for x in users]
    conn = MySQLdb.connect('192.168.150.158', 'backend', 'rahasia123', 'ipa_main')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    solr = eBsolr('http://192.168.150.105:8983/solr/twitter_tweet', 'config.conf')
    for user in users:
        _id, gender, tweet = user.split('|')
        sql = 'SELECT * FROM twitter_account WHERE id={}'.format(_id)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
            query = 'polri AND user_id:{} AND pub_year:2019'.format(_id)
            response = solr.getDocsFacets(query, 'sentiment', rows=1)
            try:
                pos = response['facets']['sentiment']['1']
            except:
                pos = 0
            try:
                neg = response['facets']['sentiment']['-1']
            except:
                neg = 0
            try:
                neu = response['facets']['sentiment']['0']
            except:
                neu = 0

            print '{}|{}|{}|{}|{}|{}|{}'.format(_id, row['screen_name'], row['name'], gender,
                                                neg, neu, pos)
    cursor.close()
    conn.close()


save_data()

