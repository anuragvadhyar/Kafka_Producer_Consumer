#!/usr/bin/env python3
import sys
import json
from kafka import KafkaConsumer

broker = 'localhost:9092'
topic = sys.argv[3]

con = KafkaConsumer(topic, bootstrap_servers=broker)
content_store = dict()
for msg in con:
    content = msg.value.strip().decode('utf-8')
    
    if content.strip("\r\n") == "EOF":
        break
    data = content.strip("\r\n").split(' ')
    
    action=data[0]
    user = data[2]
    
    if(action=='like' or action=='comment'):
    	if user not in content_store:
    		content_store[user] = {}
    	if action not in content_store[user]:
        	content_store[user][action] = 1
    	else:
        	content_store[user][action] += 1
    elif(action=='share'):
    	
    	shares=len(data)-4
    	
    	
    
    	if user not in content_store:
    	    content_store[user] = {}
    	if action not in content_store[user]:
    	   content_store[user][action]=shares
    	else:
    	   content_store[user][action]+=shares

op=dict()
print(content_store)
for key in content_store:
	if('like' not in content_store[key] or 'share' not in content_store[key] or 'comment' not in content_store[key]):
		if(not isinstance(content_store[key],dict)):
			content_store[key]={}


		if('like' not in content_store[key]):
			content_store[key]['like']=0
		if('comment' not in content_store[key]):
			content_store[key]['comment']=0
		if('share' not in content_store[key]):
			content_store[key]['share']=0
				
			
		
			
	#if('like' in content_store[key] and 'share' in content_store[key] and 'comment' in content_store[key]):
	op[key]=(content_store[key]['like']+20*(content_store[key]['share'])+5*(content_store[key]['comment']))/1000


print(json.dumps(op, indent=2, sort_keys=True))
con.close()
