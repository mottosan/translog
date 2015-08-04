from translog import app

import argparse

parser = argparse.ArgumentParser(description='Start flask app')

parser.add_argument('--debug', action="store_true", dest="debug", default=False)
parser.add_argument('--port', action="store", dest="port", type=int, default=9000)
parser.add_argument('--host', action="store", dest="host", type=str, default="localhost")

def start_translog():
	args = parser.parse_args()
	print "Starting server..."
	app.run(debug=args.debug, port=args.port, host=args.host)


import requests
import time
import random
from random import randrange
from datetime import datetime, timedelta

d1 = datetime.strptime('20150101 000000', '%Y%m%d %H%M%S')
d2 = datetime.strptime('20150715 235959', '%Y%m%d %H%M%S')

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def load_sample_data():
	tickers = ["NFLX", "MSFT", "GOOG", "F", "TSLA"]
	op = ["BUY", "SELL"]
	headers = {"Content-Type" : "application/json"}
	for i in range(0, 1000):
		adddate = bool(random.getrandbits(1))
		data = {'ticker' : random.choice(tickers), 'op' : random.choice(op), 'price' : random.randint(10, 50)}
		if adddate:
			date = random_date(d1, d2)
			data['date'] = date.strftime("%Y%m%d")
			data['time'] = date.strftime("%H%M%S")
		r = requests.post("http://localhost:9000/translog/transactions", json=data, headers=headers)
		print r.text