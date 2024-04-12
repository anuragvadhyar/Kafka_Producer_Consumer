#!/usr/bin/env python3
import sys
from kafka import KafkaProducer

broker = 'localhost:9092'
prod=KafkaProducer(bootstrap_servers=broker)
t1=sys.argv[1]
t2=sys.argv[2]
t3=sys.argv[3]

for i in sys.stdin:
	
	content=i.strip().split(' ')
	if(content[0]=='EOF'):
		prod.send(t1, value="EOF".encode('utf-8'))
		prod.send(t2, value="EOF".encode('utf-8'))
		prod.send(t3, value="EOF".encode('utf-8'))
	else:
		prod.send(t3,value=i.encode('utf-8'))
		
		
		if (content[0]==t1 or content[0]==t2):
			prod.send(content[0],value=i.encode('utf-8'))
		
		
	
	




prod.close()
