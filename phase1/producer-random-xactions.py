from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        data = {'custid' : 55,
            'type': 'dep',
            'date': int(time.time()),
            'amt': random.randint(10,101)*100,
            }
        return data

    def doLoop(self, n=1000):
        for e in range(n):
            data = self.emit()
            print(data)
            self.producer.send('bank-customer-events', value=data)
            sleep(2)

p = Producer()
p.doLoop(n=5)
