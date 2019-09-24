from library.eBsolr import eBsolr
import MySQLdb
conn = MySQLdb.connect('192.168.150.154', 'backend', 'rahasia123', 'mm_prod_news')
cursor = conn.cursor()

_path = '/home/aditya/ISA/'
_file = '201902b.csv'

with open('{}{}'.format(_path, _file), 'r') as f:
    data = f.readlines()

solr = eBsolr('http://192.168.150.126:8983/solr/news', 'config.conf')

for d in data:
    _solr_status = 'not ok'
    _sql_status = 'not ok'
    isa_id, media, title = d.replace('\n', '').split('|')
    query = 'title:"{}" AND media:{}'.format(title, media.replace(' ', '+'))
    response = solr.getDocs(query, rows=1, fields='id')
    if response['count'] > 0:
        _solr_status = 'ok'
        _id = response['docs'][0]['id']
        sql = 'SELECT n_id FROM news WHERE n_id="{}"'.format(_id)
        cursor.execute(sql)
        row = cursor.fetchone()
        if row:
            _sql_status = 'ok'
    print '{}|{}|{}|{}'.format(media, title, _solr_status, _sql_status)
    
cursor.close()
conn.close()