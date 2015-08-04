import random
from datetime import datetime, timedelta
from flask import request, abort
from flask import Flask
from flask import jsonify
import uuid
import time
import json
from  redis import StrictRedis
app = Flask(__name__)
redis = StrictRedis()

redis.set('unique_key', 0)


@app.route('/translog/transactions', methods=['GET'])
def get_transactions():
    keys = redis.keys('transaction:document:*')
    transactions = []
    for key in keys:
        transactions.append(redis.hgetall(key))
    return jsonify({'count' : len(transactions), 'transactions': transactions})


@app.route('/translog/transactions/<string:transaction_id>', methods=['GET'])
def get_transaction(transaction_id):
    return jsonify({'count' : 1, 'transaction': redis.hgetall('transaction:document:'+transaction_id)})



@app.route('/translog/transactions', methods=['POST'])
def add_transaction():
    try:
        trans_id = str(redis.incr('unique_key'))
        print trans_id
        pipe = redis.pipeline()
        data = request.json
        dt = data.get('date', None)
        ti = data.get('time', None)
        t = None
        if not (dt and ti):
            t = int(time.time())
        else:
            t = datetime.strptime("{} {}".format(dt, ti), "%Y%m%d %H%M%S").strftime("%s")
        transaction = {
            'id' : trans_id,
            'ticker' : data['ticker'],
            'price' : data['price'],
            'op' : data['op'],
            'timestamp' : t
        }
        pipe.hmset('transaction:document:'+trans_id, transaction)
        pipe.sadd('transaction:ticker:'+data['ticker'], trans_id)
        pipe.sadd('transaction:op:'+data['op'], trans_id)
        pipe.zadd('transaction:timestamp', t, trans_id)
        pipe.execute()

        return jsonify({'transaction' : transaction}), 201
    except Exception, e:
        import traceback
        traceback.print_exc()
        return jsonify({"error" : traceback.format_exc()})

@app.route('/translog/transactions/ticker/<string:ticker>', methods = ['GET'])
def get_by_ticker(ticker):
    try:
        transactions = []
        trans_ids = redis.smembers("transaction:ticker:"+ticker)
        for id in trans_ids:
            transactions.append(redis.hgetall("transaction:document:"+id))
        return jsonify({'transactions' : transactions})
    except Exception, err:
        import traceback
        traceback.print_exc()
        return jsonify({"error" : traceback.format_exc()})

def parse_duration(duration):
    if not duration:
        return 24 * 60 * 60
    else:
        unit = duration[-1]
        if unit.lower() not in ['y', 'm', 'd', 'M', 's']:
            raise Exception("Unable to parse duration "+duration)
        else:
            multiplier = 1
            if unit  == 'y':
                multiplier = 365*24*60*60
            elif unit == 'm':
                multiplier = 30*24*60*60
            elif unit == 'd':
                multiplier = 24 * 60 * 60
            elif unit == 'M':
                multiplier = 60 * 60
        return int(duration[0:-1]) * multiplier

@app.route('/translog/query', methods=["GET"])
def query():
    try:
        transactions = []
        ticker = request.args.get('ticker')
        op = request.args.get('op')
        time = request.args.get('time')
        duration = request.args.get('duration')
        duration = parse_duration(duration)
        date = request.args.get('date')
        clauses = []
        if ticker:
            clauses.append("transaction:ticker:"+ticker)
        if op:
            clauses.append("transaction:op:"+op)
        ids = None
        if date:
            if not time:
                time = "000000"
            start_timestamp = datetime.strptime("{} {}".format(date, time), "%Y%m%d %H%M%S")
            delta = timedelta(seconds=duration)
            end_timestamp = start_timestamp + delta
            clauses.append("transaction:timestamp")
            redis.zinterstore("temp_result", clauses, aggregate="MAX")
            ids = redis.zrangebyscore("temp_result", start_timestamp.strftime("%s"), end_timestamp.strftime("%s"))
        else:
            ids = redis.sinter(clauses)
        for id in ids:
            transactions.append(redis.hgetall("transaction:document:"+id))
        return jsonify({'count' : len(transactions), 'transaction' : transactions})
    except Exception, err:
        import traceback
        traceback.print_exc()
        return jsonify({"error" : traceback.format_exc()})

def run(debug=True, port=9000, host="0.0.0.0"):
    app.run(debug=True, port=9000, host="0.0.0.0")

if __name__ == "__main__":
    run(debug=True, port=9000, host="0.0.0.0")


