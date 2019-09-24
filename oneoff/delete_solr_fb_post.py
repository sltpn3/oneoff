from eBsolr import eBsolr
import MySQLdb

_MONTHS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
_HOURS = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
          "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]
_DAYS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
         "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22",
         "23", "24", "25", "26", "27", "28", "29", "30", "31"]
_YEARS = ["2010", "2011", "2012", "2013"]

solr = eBsolr('http://10.11.12.153:8983/solr/fb_post', 'config')
# conn = MySQLdb.connect('192.168.21.24', 'backend', 'rahasia123', 'ipa_facebook')
# cursor = conn.cursor(MySQLdb.cursors.DictCursor)

for year in _YEARS:
    for month in _MONTHS:
        for day in _DAYS:
            query = "d_day:{}{}{}".format(year, month, day)
            solr.deleteByQuery(query)
            print query
            
#             docs = solr.getDocs(query, 'id')['docs']
#             print '{}:{} doc(s)'.format(query, len(docs))
#             for doc in docs:
#                 sql = """SELECT * FROM fb_post WHERE post_id = '{}'""".format(doc['id'])
#                 cursor.execute(sql)
#                 row = cursor.fetchone()
#                 if not row:
#                     query = "id:{}".format(doc['id'])
#                     solr.deleteByQuery(query)
#                     print 'delete posts {}'.format(doc['id'])
#                 else:
#                     print 'post {} exists'.format(doc['id'])
