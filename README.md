This project aims at creating a Kafka producer consumer message passing stream, where consumers subscribe to topics generated by the producer and process the data retrieved.
There are 3 topics generated by the producer and 3 consumers.
### Project Summary: Kafka-based Social Media Data Processor

This project consists of a Kafka-based system for processing social media data, divided into three main components: a producer and three different consumers. Each component has a specific role in processing and analyzing the data. 

#### Components Overview:

1. **Producer (`producer.py`)**
2. **Consumer 1 (`consumer1.py`)**
3. **Consumer 2 (`consumer2.py`)**
4. **Consumer 3 (`consumer3.py`)**

### Producer

The `producer.py` script reads from standard input, processes the input data, and sends it to the appropriate Kafka topics based on the content. It is designed to work with three Kafka topics specified as command-line arguments.

**Implementation Details:**
- **Setup Kafka Producer:**
  ```python
  from kafka import KafkaProducer
  prod = KafkaProducer(bootstrap_servers='localhost:9092')
  ```
- **Read from Standard Input:**
  ```python
  for i in sys.stdin:
      content = i.strip().split(' ')
  ```
- **Send to Kafka Topics:**
  ```python
  if content[0] == 'EOF':
      prod.send(t1, value="EOF".encode('utf-8'))
      prod.send(t2, value="EOF".encode('utf-8'))
      prod.send(t3, value="EOF".encode('utf-8'))
  else:
      prod.send(t3, value=i.encode('utf-8'))
      if content[0] == t1 or content[0] == t2:
          prod.send(content[0], value=i.encode('utf-8'))
  ```

### Consumer 1

The `consumer1.py` script reads messages from a specified Kafka topic and processes the data to store user-specific information in a dictionary. It then outputs this data in a structured JSON format.

**Implementation Details:**
- **Setup Kafka Consumer:**
  ```python
  from kafka import KafkaConsumer
  con = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
  ```
- **Process Messages:**
  ```python
  for msg in con:
      content = msg.value.strip().decode('utf-8')
      if content.strip("\r\n") == "EOF":
          break
      data = content.split(' ')
      user = data[2]
      post_id = data[3]
      if user not in content_store:
          content_store[user] = {}
      if post_id not in content_store[user]:
          content_store[user][post_id] = 1
      else:
          content_store[user][post_id] += 1
  ```
- **Output JSON:**
  ```python
  print(json.dumps(op, indent=2, sort_keys=True))
  ```

### Consumer 2

The `consumer2.py` script reads messages from another Kafka topic, processes actions (like, comment, share), and calculates a score based on the actions for each user. It outputs this data in JSON format.

**Implementation Details:**
- **Setup Kafka Consumer:**
  ```python
  from kafka import KafkaConsumer
  con = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
  ```
- **Process Messages:**
  ```python
  for msg in con:
      content = msg.value.strip().decode('utf-8')
      if content.strip("\r\n") == "EOF":
          break
      data = content.strip("\r\n").split(' ')
      action = data[0]
      user = data[2]
      if action in ['like', 'comment', 'share']:
          if user not in content_store:
              content_store[user] = {}
          if action not in content_store[user]:
              content_store[user][action] = 1
          else:
              content_store[user][action] += 1
  ```
- **Calculate Scores and Output JSON:**
  ```python
  for key in content_store:
      if 'like' not in content_store[key] or 'share' not in content_store[key] or 'comment' not in content_store[key]:
          if not isinstance(content_store[key], dict):
              content_store[key] = {}
          if 'like' not in content_store[key]:
              content_store[key]['like'] = 0
          if 'comment' not in content_store[key]:
              content_store[key]['comment'] = 0
          if 'share' not in content_store[key]:
              content_store[key]['share'] = 0
      op[key] = (content_store[key]['like'] + 20 * (content_store[key]['share']) + 5 * (content_store[key]['comment'])) / 1000
  print(json.dumps(op, indent=2, sort_keys=True))
  ```

### Consumer 3

The `consumer3.py` script reads messages from another Kafka topic and processes them to store user-specific actions. It calculates a composite score for each user based on likes, comments, and shares, and outputs this data in JSON format.

**Implementation Details:**
- **Setup Kafka Consumer:**
  ```python
  from kafka import KafkaConsumer
  con = KafkaConsumer(topic, bootstrap_servers='localhost:9092')
  ```
- **Process Messages:**
  ```python
  for msg in con:
      content = msg.value.strip().decode('utf-8')
      if content.strip("\r\n") == "EOF":
          break
      data = content.strip("\r\n").split(' ')
      action = data[0]
      user = data[2]
      if action in ['like', 'comment', 'share']:
          if user not in content_store:
              content_store[user] = {}
          if action not in content_store[user]:
              content_store[user][action] = 1
          else:
              content_store[user][action] += 1
  ```
- **Calculate Scores and Output JSON:**
  ```python
  for key in content_store:
      if 'like' not in content_store[key] or 'share' not in content_store[key] or 'comment' not in content_store[key]:
          if not isinstance(content_store[key], dict):
              content_store[key] = {}
          if 'like' not in content_store[key]:
              content_store[key]['like'] = 0
          if 'comment' not in content_store[key]:
              content_store[key]['comment'] = 0
          if 'share' not in content_store[key]:
              content_store[key]['share'] = 0
      op[key] = (content_store[key]['like'] + 20 * (content_store[key]['share']) + 5 * (content_store[key]['comment'])) / 1000
  print(json.dumps(op, indent=2, sort_keys=True))
  ```

