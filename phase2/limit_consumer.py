import sqlalchemy as sqlalchemy
from kafka import KafkaConsumer, TopicPartition
from json import loads
from contextlib import closing
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
from sqlalchemy import Integer, String, Column, Sequence, create_engine, table, VARCHAR

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        self.limit = -5000
        # self.wth_sum = 0
        # self.dep_sum = 0
        self.total = 0
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.engine = create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/kafka',
                                    encoding='latin1', echo=True)
        self.conn = self.engine.connect()

        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.total += message['amt']
            if message['type'] == 'wth' and self.total < self.limit:
                print(f"amt total:{self.total} is less than or equal to the limit of -5000:")
            # if message['type'] == 'wth' and self.custBalances[message['custid']] < self.limit:
            #     print('withdraw limit reach')
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.total -= message['amt']
            # print(self.custBalances)
            # for self.custBalances[message['custid']] in self.custBalances:
            #     if message['amt'] <= self.limit:
            #         print(f"cust. id {message['custid']}: minimum balance exceeded")
            # for key, value in self.custBalances.items():
            #     if value <= self.limit:
            #         print(f"amt total:{value} is less than or equal to the limit of -5000:")
            #         print(key, value)



if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()