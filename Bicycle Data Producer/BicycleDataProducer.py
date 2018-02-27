from random import randrange, random
from datetime import datetime, date, time
import threading, time

from kafka import KafkaProducer, Serializer
from kafka.errors import KafkaError

# Get the Kafka broker node
brokers = "localhost:9092"

# Get the exists topic named welcome-message
topic = "mytesttopic"

events = 0

intervalEvent = 30  # in second

rndStart = 0  # in second

rndEnd = 500  # in second

clientId = randrange(10000)

tup = ('Trek', 'Giant', 'Jett', 'Cannondale', 'Surly')

i = 0

while True:
    n = rndStart + randrange(rndEnd - rndStart + 1)
    for i in range(0, n):
        today = datetime.today()
        producer = KafkaProducer(bootstrap_servers=[brokers])
        value = str(today) + ","+tup[randrange(4)]
        producer.send(topic, value.encode("utf-8"))
        producer.flush()
        producer.close()

    k = i + 1
    if intervalEvent > 0:
        time.sleep(intervalEvent * 1000)
        i += 1

    if events > 0:
        print("event>0")
    elif i == events:
        print("i==events")
        break
