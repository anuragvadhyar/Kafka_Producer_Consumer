#!/usr/bin/env python3
import sys
import json
from kafka import KafkaConsumer

broker = 'localhost:9092'
topic = sys.argv[2]

con = KafkaConsumer(topic, bootstrap_servers=broker)
content_store = {}  # Initialize an empty dictionary

for msg in con:
    content = msg.value.strip().decode('utf-8')
    if content.strip("\r\n") == "EOF":
        break
    data = content.split(' ')
    

    user = data[2]
    post_id = data[3]

    # Check if user exists in content_store and initialize if not
    if user not in content_store:
        content_store[user] = {}
    
    # Check if post_id exists for the user and initialize if not
    if post_id not in content_store[user]:
        content_store[user][post_id] = 1
    else:
        content_store[user][post_id] += 1

op = {}

for key, val in content_store.items():
    op[key] = {post_id: count for post_id, count in val.items()}

print(json.dumps(op, indent=2, sort_keys=True))

con.close()
