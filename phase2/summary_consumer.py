from kafka import KafkaConsumer, TopicPartition
from json import loads


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
        self.amt = []
        self.amt_total = sum(self.amt)
        self.wth_sum = 0
        self.dep_sum = 0
        self.dep_tot = []
        self.wth_tot = []
        self.dep_count = len(self.dep_tot)
        self.wth_count = len(self.wth_tot)
        dep_mean = self.dep_sum / self.dep_count 
        wth_mean = self.wth_sum / self.wth_count


        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        # Go back to the readme.

    def handleMessages(self):
        global dep_count
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.dep_sum += message['amt']
                self.dep_tot.append(message)
                dep_mean = self.dep_sum/len(self.dep_tot)
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.wth_sum += message['amt']
                self.wth_tot.append(message)
                wth_mean = self.wth_sum/len(self.wth_tot)
            print(self.custBalances, self.dep_sum, self.wth_sum, dep_mean, wth_mean)
            # return message
            # for message in

    # def mean_amt(self):
    #     for message in self.consumer:
    #         message = message.value
    #         if message['type'] == 'wth' or 'dep':
    #             self.amt_total.append(message['type'].value)
    #         print(self.amt_total


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
    # c.mean_amt()
