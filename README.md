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

to create a new kafka topic
```bash
kafka-topics --create \
--zookeeper localhost:2181 \
--replication-factor 1 \
--partitions 1 \
--topic bank-customer-events
```

and whose SQLAlchemy might be
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

 ## Phase 2
     
transaction consumers
    a user by user put into a db
    a memory based ledger of bank's total deposits
