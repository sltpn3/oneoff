import re
import MySQLdb
import json
from datetime import datetime, date

conn = MySQLdb.connect('192.168.180.76', 'backend', 'rahasia123', 'mm_prod_news')
cursor = conn.cursor(MySQLdb.cursors.DictCursor)

_files = ['imm_log', 'imm_log-20190825', 'imm_log-20190901', 'imm_log-20190908', 'imm_log-20190915']
_path = '/home/aditya/imm_log_archive/'

all_logs = []

for _file in _files:
    with open('{}{}'.format(_path, _file), 'r') as f:
        lines = f.readlines()
        for line in lines:
            all_logs.append(line.replace('\n', ''))

all_data = []

user_dict = {}

def is_int(value):
    try:
        int(value)
        return True
    except:
        return False
    
# print is_int('arid')

for log in all_logs:
    try:
        data = {}
        data['_time'] = re.findall('\[([\w:/]+\s[+\-]\d{4})\]', log)[0]
        log = log.replace("[{}]".format(data['_time']), '')
        result = re.findall('\"(GET|POST|DELETE|OPTIONS|PUT|HEAD|CONNECT|TRACE|PATH)(.*?)\"', log)[0]
        data['request_path'] = ''.join(list(result))
        log = log.replace('"{}"'.format(data['request_path']), '')

        ip_user, others = log.split('   ')
        data['others'] = others
        ip, user_id = ip_user.split(' - ')
        data['ip'] = ip
        data['user_id'] = user_id
        if user_id == '-':
            data['user'] = 'not_available'
        else:
            if user_id in user_dict:
                data['user'] = user_dict[user_id]
            else:
                sql = 'SELECT u_username FROM user WHERE u_id = {}'.format(data['user_id'])
                cursor.execute(sql)
                row = cursor.fetchone()
                data['user'] = row['u_username']
                user_dict[data['user_id']] = data['user']
#         result = re.findall('   (.*)?', log)[0]
#         print result
#         print log
        all_data.append(data)
#         print data
    except Exception, e:
        pass

# f = open('{}all_data.json', 'w')
# f.write(json.dumps(all_data))
# f.close()

daily_data = {}

for data in all_data:
    _date = datetime.strptime(data['_time'], '%d/%b/%Y:%H:%M:%S +0700').strftime('%Y%m%d')
    if _date not in daily_data:
        daily_data[_date] = {}
    try:
        daily_data[_date][data['user']] += 1
    except:
        daily_data[_date][data['user']] = 1

for _day in daily_data:
    print '{}|'.format(_day)
    for user in daily_data[_day]:
        print '{}|{}'.format(user, daily_data[_day][user])
# print daily_data
#     try:
#         s = '{}|{}|{}|{}|{}|{}'.format(data['ip'], data['_time'].replace(' +0700', ''), data['user_id'], data['user'], data['request_path'], data['others'])
#         print s
#     except Exception, e:
#         print e
#         print data
        