import nsq
import urlparse
import json
import base64
import requests
from pprint import pprint

class Producer:
    def __init__(self, host, port, topic):
        self.base_url = "http://{}:{}".format(host, port)
        self.topic = topic

    def put_message(self, msg):
        params = {"message": msg}
        url = "{}?topic={}".format(urlparse.urljoin(self.base_url, "pub"), self.topic)
        requests.post(url, params)


def handler1(message):
    producer = Producer('192.168.21.26', '4151', 'sna_facebook_post2')
    print 'moving'
    producer.put_message(dict(urlparse.parse_qsl(message.body))["message"])
#     print urlparse.parse_qsl(message.body)
#     msg_body = dict(urlparse.parse_qsl(message.body))["message"]
#     json_raw = base64.b64decode(msg_body)
#     post = json.loads(json_raw)
#     print post
    return True


def handler2(message):
#     print message.body
#     print urlparse.parse_qsl(message.body)
    msg_body = dict(urlparse.parse_qsl(message.body))["message"]
#     print msg_body
    json_raw = base64.b64decode(msg_body[2:-1])
#     print json_raw
    post = json.loads(json_raw)
    print post
    return True


def handler3(message):
#     print message.body
#     print urlparse.parse_qsl(message.body)
#     pprint(dict(urlparse.parse_qsl(message.body))["message"])
    post = json.loads(dict(urlparse.parse_qsl(message.body))["message"])
    post_message = {'picture': '', 
                    'comment_from': 'Rudi', 
                    'fb_status_id': '156483584365269_2541809589165978', 
                    'description': '', 
                    'comment_from_id': '100000149302322', 
                    'count_likes': '', 
                    'fb_id': '156483584365269', 
                    'post_name': '', 
                    'caption': '', 
                    'count_share': '', 
                    'post_type': 'account', 
                    'date_created': 1545150746, 
                    'count_comments': '', 
                    'link': 'https://facebook.com/156483584365269/posts/2541809589165978', 
                    'message': 'Beda hati nurani, presiden skrg terlalu baik hati.. coba pembuat hoax langsung di tembak di tempat aja... hukum terlalu tumpul ke bwh... hukum indonesia bisa di beli pake uang, rakyat kecil harus menderita... apalagi uang pajak yg disetorin mala di korupsi besar besaran..', 
                    'type': post['type'], 
                    'id': post['id']}
#     post = json.loads(json_raw)
#     if post['type'] == 'photo':
#         print post
    return True

def handler4(message):
#     print message.body
#     print urlparse.parse_qsl(message.body)
    msg_body = dict(urlparse.parse_qsl(message.body))["message"]
    print msg_body
#     json_raw = base64.b64decode(msg_body[2:-1])
#     print json_raw
#     post = json.loads(json_raw)
#     print post
    return True


# nsqd_tcp_addresses = "192.168.21.26:4150"
# nsq.Reader(message_handler=handler1, topic='sna_facebook_post',
#            channel='test', lookupd_poll_interval=15,
#            nsqd_tcp_addresses=[nsqd_tcp_addresses],
#            lookupd_request_timeout=60,
#            max_in_flight=15)

# nsqd_tcp_addresses = "192.168.21.26:4150"
# nsq.Reader(message_handler=handler3, topic='sna_facebook_post2',
#            channel='archive', lookupd_poll_interval=15,
#            nsqd_tcp_addresses=[nsqd_tcp_addresses],
#            lookupd_request_timeout=60,
#            max_in_flight=15)

nsqd_tcp_addresses = "192.168.150.15:4150"
nsq.Reader(message_handler=handler4, topic='twitter-streaming',
           channel='ipa-channel', lookupd_poll_interval=15,
           nsqd_tcp_addresses=[nsqd_tcp_addresses],
           lookupd_request_timeout=60,
           max_in_flight=15)

nsq.run()
