from pprint import pprint
import json

with open('post1.json', 'r') as f:
    raw = f.read()

# print raw
post = json.loads(raw)

# pprint(post)
pictures = []
for attachment in post['attachments']['data'][0]['subattachments']['data']:
#     pprint(attachment)
    if attachment['type'] == 'photo':
        pictures.append(attachment['media']['image']['src'])

print pictures