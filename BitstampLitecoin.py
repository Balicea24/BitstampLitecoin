import pysher
import logging
import sys
import time
import psycopg2
import json

root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
root.addHandler(ch)

conn = psycopg2.connect(database="postgres", user = "postgres", password = "Roflmao24!", host = "127.0.0.1", port = "5432")
cur = conn.cursor()

cur.execute('''CREATE TABLE TRADES
      (ID SERIAL PRIMARY KEY,
      UNIXTIMESTAMP INT NOT NULL,
      PRICE FLOAT(8) NOT NULL,
      AMOUNT FLOAT(8) NOT NULL,
      SELL_ORDER SMALLINT NOT NULL,
      BUY_ID BIGINT NOT NULL,
      SELL_ID BIGINT NOT NULL);''')

cur.execute('''CREATE TABLE ORDERS
     (ID SERIAL PRIMARY KEY,
      UNIXTIMESTAMP INT NOT NULL,
      PRICE FLOAT(8) NOT NULL,
      AMOUNT FLOAT(8) NOT NULL,
      SELL_ORDER SMALLINT NOT NULL,
      ORDER_ID BIGINT NOT NULL);''')

conn.commit()

class Trades:
    pusher = pysher.Pusher("de504dc5763aeef9ff52", daemon = False)
    def __init__(self):
        self.pusher.connection.bind('pusher:connection_established', self.listen)
        self.pusher.connect()

    def listen(self, data):
        channel = self.pusher.subscribe('live_trades_ltcusd')
        channel.bind('trade', self.get)

    def get(self, *args):
        jsonify = args[0].split("'")[0]
        data = json.loads(jsonify)
        self.push_to_DB(data)

    def push_to_DB(self, data):
        unixtimestamp = int(data["datetime"])
        price = data['price_str']
        amount = data['amount']
        sel_order = data['type']
        buy_id = data['buy_order_id']
        sell_id = data['sell_order_id']

        cur.execute("INSERT INTO TRADES (UNIXTIMESTAMP,PRICE,AMOUNT,SELL_ORDER,BUY_ID,SELL_ID) \
              VALUES (%s, %s, %s, %s, %s, %s)", (unixtimestamp, price, amount, sel_order, buy_id, sell_id));

        conn.commit()

class Orders(Trades):
    pusher = pysher.Pusher("de504dc5763aeef9ff52", daemon = False)
    def listen(self, data):
        channel = self.pusher.subscribe('live_orders_ltcusd')
        channel.bind('order_created', self.get)

    def push_to_DB(self, data):
        unixtimestamp = int(data["datetime"])
        price = data["price"]
        amount = data["amount"]
        sel_order = data["order_type"]
        order_id = data['id']

        cur.execute("INSERT INTO ORDERS (ORDER_ID,UNIXTIMESTAMP,PRICE,AMOUNT,SELL_ORDER) \
              VALUES (%s, %s, %s, %s, %s)", (order_id, unixtimestamp, price, amount, sel_order));

        conn.commit()

Trades(), Orders()
