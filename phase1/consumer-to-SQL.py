import sqlalchemy as sqlalchemy
from kafka import KafkaConsumer, TopicPartition
from json import loads
from contextlib import closing
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
from sqlalchemy import Integer, String, Column, Sequence, create_engine, table, VARCHAR


Base = declarative_base()

# transaction_autoincr_seq = Sequence('trans_autoincr_seq')

class Transaction(Base):
    __tablename__ = 'transaction'
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(VARCHAR(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

    def __repr__(self):
        ...
        return "<Transaction(id='%s', custid='%s', type='%s', date='%s', amt='%s',)>" % (
            self.id, self.custid, self.type, self.date, self.amt)



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
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.engine = create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/kafka',
                               encoding='latin1', echo=True)
        self.conn = self.engine.connect()


        # session.add(message)


    # try:
    #     with closing(create_session()) as db_session:
    #         name = db_session.query(Transaction.name).filter_by(id=5).one()[0]
    #     # Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            ch_message = list(message.values())
            new_values = tuple(ch_message)
            self.conn.execute("INSERT INTO transaction VALUES (%s,%s,%s,%s)", new_values)
            # message_psql = Transaction(custid=message['custid'], type=message['type'], date=message['date'], amt = message['amt'])
            # Session = sessionmaker(bind=self.engine)
            # session = Session()
            # session.add(message_psql)
            # session.commit()
            # add message to the transaction table in your SQL using SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
