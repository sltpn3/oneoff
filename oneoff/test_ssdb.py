from library.ssdb import SSDB
import json


# print response

def process_response(response):
    if response[0:8] == 'ok None':
        print response
        
ssdb = SSDB('192.168.99.218', 8888)
response = ssdb.request('qpop', ['imm_facebook'])

print json.loads(response.data)