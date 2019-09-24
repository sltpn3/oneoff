import MySQLdb

source_conn = MySQLdb.connect('192.168.150.158', 'backend', 'rahasia123', 'ipa_main')
source_cursor = source_conn.cursor(MySQLdb.cursors.DictCursor)

target_conn = MySQLdb.connect('192.168.150.22', 'backend', 'rahasia123', 'ipa_main')
target_cursor = target_conn.cursor(MySQLdb.cursors.DictCursor)

run = True
limit = 10000
start = 0
while run:
    print start
    rows_to_insert = []
    sql = """SELECT * FROM twitter_account ORDER BY id LIMIT {}, {}""".format(start, limit)
    source_cursor.execute(sql)
    rows = source_cursor.fetchall()
    start += len(rows)
    if len(rows) < limit:
        run = False
    for row in rows:
        row_to_insert = """({}, {}, {}, {}, {}, {}, {}, {}, {} ,{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                        """.format(row['id'],
                                   "'{}'".format(MySQLdb.escape_string(row['screen_name'])) if row['screen_name'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['name'])) if row['name'] else 'NULL',
                                   "'{}'".format(row['created_at']) if row['created_at'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['profile_image_url'])) if row['profile_image_url'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['description'])) if row['description'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['t_location'])) if row['t_location'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['location'])) if row['location'] else 'NULL',
                                   "{}".format(row['statuses_count']) if row['statuses_count'] else 'NULL',
                                   "{}".format(row['friends_count']) if row['friends_count'] else 'NULL',
                                   "{}".format(row['followers_count']) if row['followers_count'] else 'NULL',
                                   "{}".format(row['favourites_count']) if row['favourites_count'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['url'])) if row['url'] else 'NULL',
                                   "{}".format(row['verified']) if row['verified'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['time_zone'])) if row['time_zone'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['protected'])) if row['protected'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['language'])) if row['language'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['age'])) if row['age'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['gender'])) if row['gender'] else 'NULL',
                                   "'{}'".format(MySQLdb.escape_string(row['religion'])) if row['religion'] else 'NULL')
#         print row_to_insert
        rows_to_insert.append(row_to_insert)

    sql = """INSERT IGNORE INTO twitter_account VALUES {}""".format(','.join(rows_to_insert))
    target_cursor.execute(sql)
    target_conn.commit()

