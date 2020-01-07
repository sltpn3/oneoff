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

# s = """#mysqldump -h 10.11.12.35 -u root fb_master fb_master_posts --insert-ignore --where="cache_id BETWEEN {} AND {}" --skip-add-drop-table |gzip -c > json_cache.{:02d}.sql.gz"""

s = """'mysqldump -h 10.11.12.35 -u root -p rahasia123 fb_master fb_master_posts --insert-ignore --skip-add-drop-table -w "insert_date BETWEEN '{} 00:00:00' AND '{} 23:59:59'"|gzip -c > fb_master_posts.20191226.{:03d}.sql.gz'"""

start = 0
incr = 0000000
end = 400000000

# _count = 1
# 
# run = True
# while run:
#     print s.format(start, _count)
#     start += incr
#     _count += 1
#     if start > end:
#         run = False


count = 1
run = True
d = datetime(2017, 4, 1)
diff = (datetime.now() - d).days
print diff

while run:
#     print s.format(d.strftime('%Y-%m-%d'), count)
#     print d 
    
    if d.day == 1:
        if d.month in [1, 3, 5, 7, 8, 10, 12]:
            last_day = 31
        elif d.month in [4, 6, 9, 11]:
            last_day = 30
        else:
            if d.year%4 == 0:
                last_day = 29
            else:
                last_day = 28
        
        print s.format(d.strftime('%Y-%m-%d'), d.strftime('%Y-%m-{}').format(last_day), count)
        count += 1
    d += timedelta(days=1)

    
    if d.year == 2020:
        run = False
#     if count > diff:
#         run = False