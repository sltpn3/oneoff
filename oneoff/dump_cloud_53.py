from datetime import datetime, timedelta
# s = 'mysqldump -h 10.11.12.53 -uroot -prahasia123 --skip-add-drop-table --insert-ignore clipper_news data_news --where="news_pubday = \'{0}-{1:02d}-{2:02d}\'"|gzip -c > data_news.{0}{1:02d}{2:02d}.sql.gz'
# 
# d = datetime(2009, 1, 2)
# run = True
# 
# while run:
#     print s.format(d.year, d.month, d.day)
#     d += timedelta(days=1)
#     if d > datetime.now():
#         run = False

s = 'mysqldump -h 127.0.0.1 -u root -prahasia123 opinion json_cache2 -w "cache_id BETWEEN {} AND {}" --insert-ignore --skip-add-drop-table |gzip -c > opinion.20190918.my.{:02d}.sql.gz'

_id = 0
_count = 1

run = True
while run:
    print s.format(_id, _id+4000000, _count)
    _id += 4000000
    _count += 1
    if _id > 45000000:
        run = False
