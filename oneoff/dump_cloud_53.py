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

# s = 'mysqldump -h 127.0.0.1 -u root -prahasia123 opinion json_cache2 -w "cache_id BETWEEN {} AND {}" --insert-ignore --skip-add-drop-table |gzip -c > opinion.20190918.my.{:02d}.sql.gz'

s = """#mysqldump -h 127.0.0.1 -u root opinion json_cache_bak --insert-ignore --where="cache_id BETWEEN {} AND {}" --skip-add-drop-table |gzip -c > json_cache.{:02d}.sql.gz"""

start = 37800309
incr = 5000000
end = 372327560

_count = 1

run = True
while run:
    print s.format(start, start+incr, _count)
    start += incr
    _count += 1
    if start >= end:
        run = False
