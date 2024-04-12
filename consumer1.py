#!/usr/bin/env python3
import sys
import json
from kafka import KafkaConsumer

broker = 'localhost:9092'
topic = sys.argv[1]

con = KafkaConsumer(topic, bootstrap_servers=broker)
content_store = dict()

for msg in con:
    content = msg.value.strip().decode('utf-8')

    data = []
    word = ''
    inside_quotes = False
    if content.strip("\r\n") == "EOF":
        break

    for char in content:
        if char == ' ' and not inside_quotes:
            if word:
                data.append(word)
            word = ''
        elif char == '"':
            inside_quotes = not inside_quotes
        else:
            word += char

    # Add the last word (if any)
    if word:
        data.append(word)

    user = data[2]
    if user not in content_store:
        content_store[user] = [x.replace("\\", "") for x in [data[-1]]]
        
    else:
        content_store[user].append(data[-1].replace("\\", ""))
        

op = dict()

for key, val in content_store.items():
    op[key] = [x.replace("\\", "") for x in val]

print(json.dumps(op, indent=2, sort_keys=True))

con.close()
