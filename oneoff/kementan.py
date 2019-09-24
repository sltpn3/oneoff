from eBsolr import eBsolr
from urlparse import urlparse
import MySQLdb
from datetime import datetime, timedelta
import json

q = '("Kementerian Pertanian" OR "kementrian pertanian" OR "kementan" OR "menteri pertanian" OR "mentri pertanian" OR "mentan" OR "Menteri Amran Sulaiman" OR "Direktorat Jenderal Prasarana & sarana pertanian" OR "Direktorat jenderal tanaman pangan" OR "ditjen tanaman pangan" OR "Direktorat jenderal Holtikultura" OR "Ditjen holtikultura" OR "Direktorat jenderal perkebunan" OR "ditjen perkebunan" OR "Direktorat jenderal peternakan & kesehatan hewan" OR "Badan penelitian & pengembangan pertanian" OR "badan penelitian & pengembangan sumber daya manusia pertanian" OR "Badan ketahanan pangan" OR "Badan karantina pertanian" OR "pusat data & sistem informasi pertanian" OR "Pusat perlindungan varietas tanaman & perizinan pertanian" OR "Pusat perpustakaan & penyebaran teknologi pertanian" OR "Pusat sosial ekonomi dan Kebijakan Pertanian" OR "Staf ahli bidang pengembangan bio industri" OR "Badan Penyuluhan dan Pengembangan Sumber Daya Manusia Pertanian" OR "PPSDMP" OR "Sekretariat Jenderal" OR "Inspektorat Jenderal" OR "Ditjen Tanaman Pangan" OR "Ditjen Hortikultura" OR "Ditjen Perkebunan" OR "Ditjen Peternakan dan Kesehatan Hewan" OR "Ditjen Prasarana dan Sarana Pertanian" OR "Badan Penelitian dan Pengembangan Pertanian" OR "Balitbangtan" OR "Badan Ketahanan Pangan" OR "Badan Karantina Pertanian" OR "Badan Penyuluhan dan Sumber Daya Manusia Pertanian" OR "BPPSDMP") NOT ("Asas Tani" OR "Yahya Hussin" OR "Wahid Said" OR "Tan Sri Sabri Ahmad" OR "Ashraf Ghani" OR "Azman Daim" OR "Nizam Mahshar" OR "Sarawakiana" OR "Sombir Kumar" OR "Om Prakash Dhankar" OR "Datuk Ismail Sabri" OR "Khalid Abu Bakar" OR "Datuk Seri Hishamuddin Hussein" OR "Gaddhi Kheri" OR "Ummi Hafilda" OR "Datuk Seri Mohamed Nazri Abdul Aziz")'
epoch_date = '1970-01-01 00:00:00'


def process_time(seconds):
    if seconds > 31536000:
        return '{} {} ago'.format(int(seconds / 31536000), 'years' if seconds / 31536000 > 1 else 'year')
    elif seconds > 2592000:
        return '{} {} ago'.format(int(seconds / 2592000), 'months' if seconds / 2592000 > 1 else 'month')
    elif seconds > 604800:
        return '{} {} ago'.format(int(seconds / 604800), 'weeks' if seconds / 604800 > 1 else 'week')
    elif seconds > 86400:
        return '{} {} ago'.format(int(seconds / 86400), 'days' if seconds / 86400 > 1 else 'day')
    elif seconds > 3600:
        return '{} {} ago'.format(int(seconds / 3600), 'hours' if seconds / 3600 > 1 else 'hour')
    elif seconds > 60:
        return '{} {} ago'.format(int(seconds / 60), 'minutes' if seconds / 60 > 1 else 'minute')
    else:
        return '{} seconds ago'.format(seconds)


def online():
    solr = eBsolr('http://192.168.150.125:8985/solr/news/', 'config_kementan.conf', version=4)
    conn = MySQLdb.connect('192.168.150.154', 'backend', 'rahasia123', 'mm_prod_news')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    data = solr.getDocs(q, rows=30, sort='pubDate DESC')
    result = []
    for doc in data['docs']:
        epoch_datetime = datetime.strptime(epoch_date, '%Y-%m-%d %H:%M:%S')
        sql = 'SELECT * FROM news WHERE n_id={}'.format(doc['id'])
        cursor.execute(sql)
        row = cursor.fetchone()
        now = datetime.now()
        timestamp = (row['n_date'] - timedelta(hours=7)) - epoch_datetime
#         print timestamp
        news = {"id": doc['id'],
                "title": doc['title'],
                "points": 999,
                "user": "ebdesk",
                "time": int(timestamp.total_seconds()),
                "time_ago": process_time((now - row['n_date']).total_seconds()),
                "comments_count": 99,
                "type": "online news",
                "url": row['n_link'],
                "imgurl": row['n_image'],
                "domain": urlparse(row['n_link']).netloc
              }
        result.append(news)
#         print news
    print json.dumps(result)
    cursor.close()
    conn.close()
    
def printed():
    solr = eBsolr('http://192.168.150.119:8983/solr/newspaper/', 'config_kementan.conf', version=4)
    conn = MySQLdb.connect('192.168.150.162', 'backend', 'rahasia123', 'mm_prod_printed')
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    data = solr.getDocs(q, rows=30, sort='pubDate DESC')
    result = []
    for doc in data['docs']:
        epoch_datetime = datetime.strptime(epoch_date, '%Y-%m-%d %H:%M:%S')
        sql = 'SELECT * FROM newspaper WHERE n_id={}'.format(doc['id'])
        cursor.execute(sql)
        row = cursor.fetchone()
        now = datetime.now()
        timestamp = datetime.combine(row['n_date'], datetime.min.time()) - epoch_datetime
#         print timestamp
        news = {"id": doc['id'],
                "title": doc['title'],
                "points": 999,
                "user": "ebdesk",
                "time": int(timestamp.total_seconds()),
                "time_ago": process_time((now - datetime.combine(row['n_date'], datetime.min.time())).total_seconds()),
                "comments_count": 99,
                "type": "printed news",
                "url": 'imm.ebdesk.com',
                "imgurl": 'imm.ebdesk.com/upload_scan_scaled/' + doc['url'][0],
                "domain": row['n_media']
              }
        result.append(news)
#         print news
    print json.dumps(result)
    cursor.close()
    conn.close()


printed()
