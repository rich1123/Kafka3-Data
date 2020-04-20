# Kafka3-Data
build simple consumer (python)

## ZipBank Project

### Phase 1
build a consumer to use SQLalchemy to store incoming transactions into a SQL Database. 
transaction producer
    a simple user by user generates random deposits and withdrawals

a transaction 
```
{ custid: int, type: W/D, date: now, amt: int }
```
so a sample
```
{ custid: 55, type: "Dep", date: 1587398219, amt: 10000 }
{ custid: 55, type: "Wth", date: 1587398301, amt: 2500 }
```
which means:
customer who's id is 55, Deposit, at time 1587398219, a total of $100.00
customer who's id is 55, Withdraw, at time 1587398301, a total of $25.00

to create a new kafka topic (the one you need for the phase1 scripts to work.)
```bash
kafka-topics --create \
--zookeeper localhost:2181 \
--replication-factor 1 \
--partitions 1 \
--topic bank-customer-events
```

### Your Phase1 Mession

these scripts work. kinda. the problem is every time we re-start the consumer, we lose
all the customer data. the reason is the coder doesn't know SQL like you do! so all the data gets
put into in-memory data structures, but every time you restart the consumer script, they get emptied.

you need to use SQL alchemy to add to the Consumer in phase1 what's needed to save that transaction information into a "transaction" table in the SQl of your choice.

you probably need to create the "database" and the "table" within your Sql Database, and then
connect to it anytime someone creates a XactionConsumer() object.

and that SQLAlchemy  might be something like 
``` python
class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)
 ```

 Read through the producer in phase1. See where it is generating random transaction sizes, and random on whether it's a deposit or withdrawal. (and random on what customer id is used for the transaction)


 ## Phase 2
     
transaction consumers
    a user by user put into a db
