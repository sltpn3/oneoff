from urlparse import urlparse, parse_qs
from datetime import datetime
import re
import json
import gzip
import MySQLdb
import argparse

# with gzip.open('imm.reverse.proxy.access.log-20191228.gz', 'r') as f:
#     lines = f.readlines()


def parse_line(line):
    pattern = '^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - (\d{1,}|-) (\[\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4}\]) "(GET|POST|PUT|PATCH|DELETE|OPTIONS|CONNECT|TRACE|HEAD) (.{1,}) (.{1,})" (\d{3}) (\d{1,}) "(.*)" "(.*)" (.*)'
    matches = re.findall(pattern, line)
    return matches[0]


def parse_log(_file, _day):
    conn = MySQLdb.connect('192.168.150.151', 'backend', 'rahasia123', 'nginx_log')
    cursor = conn.cursor()
    if not _day:
        _day = datetime.now().strftime('%Y%m%d')
    with gzip.open('{}-{}.gz'.format(_file, _day), 'r') as f:
        lines = f.readlines()

    for line in lines:
        try:
            ip, user, _date, method, request_body, protocol, status, bytes, referrer, browser_data, timing = parse_line(line)
            url = 'http://imm.ebdesk.com' + request_body
            parsed = urlparse(url)
            parameters = parse_qs(parsed.query)
            topic_id = None
            _date = datetime.strptime(_date[1:-6], '%d/%b/%Y:%H:%M:%S ').strftime('%Y-%m-%d %H:%M:%S')
            for k in parameters:
                if k != 'params':
                    if k == 't_id':
                        topic_id = parameters[k][0]
                else:
                    parsed_param = json.loads(parameters[k][0])
                    for k in parsed_param:
                        if k == 'tid' or k == 't_id':
                            topic_id = str(parsed_param[k]).split(',')
                            topic_id = topic_id[0]

            sql = """INSERT INTO nginx_access_log VALUES ('{}', '{}', {}, {}, '{}', '{}', '{}', '{}', '{}')
                  """.format(_date, ip,
                             'NULL' if user == '-' else "'{}'".format(user),
                             "'{}'".format(topic_id) if topic_id else 'NULL',
                             method, request_body, status, referrer, browser_data)
            cursor.execute(sql)
#             print sql
        except Exception, e:
            print e

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='Nginx Log Parser',
                                        formatter_class=argparse.RawDescriptionHelpFormatter)
    argparser.add_argument('-f', '--file', help='File', metavar='', default='imm.reverse.proxy.access.log', type=str)
    argparser.add_argument('-d', '--date', help='Date', metavar='', default=None, type=str)
    args = argparser.parse_args()

    parse_log(args.file, args.date)

