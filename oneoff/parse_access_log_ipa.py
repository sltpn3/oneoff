from pprint import pprint

with open('/home/aditya/ipa_access_log/access_log_ipa', 'r') as f:
    logs = f.readlines()

result = {}

for log in logs:
    log = log.replace('\n', '')
    _date = log[0:10]
    user = log[25:]
    year = _date[0:4]
    month = _date[5:7]
    day = _date[8:10]
    if user != '-':
        try:
            result[user][year][month][day] += 1
        except:
            if user not in result:
                result[user] = {}
            if year not in result[user]:
                result[user][year] = {}
            if month not in result[user][year]:
                result[user][year][month] = {}
            result[user][year][month][day] = 1

month = '07'

print result['MA1']

# for user in result:
#     line = '{};'.format(user)
#     for _day in range(1,32):
#         try:
#             line = '{}{};'.format(line, result[user]['2019'][month]['{:02d}'.format(_day)])
#         except:
#             line = '{}0;'.format(line)
#     print line