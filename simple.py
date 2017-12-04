'''
Created on 26 Oct 2017

@author: ywliu
'''

#!/usr/bin/env python

import sys
sys.path.append('..')

import time
import json
import requests
import pusherclient
import datetime
from cassandra.cluster import Cluster
# Add a logging handler so we can see the raw communication data
import logging
from cassandra.cluster import Cluster


root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
root.addHandler(ch)

global pusher
global cluster 
global session
global prepared_usd
global prepared_eur
def print_usage(filename):
    print("Usage: python %s <appkey>" % filename)

def channel_callback1(data):
    print("Channel1 Callback: %s" % data)
    json_data=json.loads(data)
    time_stamp=json_data['timestamp']
    #bound = prepared_usd.bind((int(time_stamp),data))
    #result=session.execute(bound)
def channel_callback2(data):
    print("Channel2 Callback: %s" % data)
    json_data=json.loads(data)
    time_stamp=json_data['timestamp']
    #bound = prepared_eur.bind((int(time_stamp),data))
    #result=session.execute(bound)
    print(result)
    #print(
    #    datetime.datetime.fromtimestamp(
    #    int(timestamp)
    #    ).strftime('%Y-%m-%d %H:%M:%S')
    #)
    #print(time.strftime("%B %d %Y", "timestamp"))
def connect_handler(data):
    channel1 = pusher.subscribe("diff_order_book")
    channel2 = pusher.subscribe("diff_order_book_btceur")
    channel1.bind('data', channel_callback1)
    channel2.bind('data', channel_callback2)
    

if __name__ == '__main__':
    
    #cluster = Cluster()
    #session = cluster.connect('marketview')
    #orderbook = json.loads(requests.get( #TODO base quote here
    #    "https://www.bitstamp.net/api/order_book/"))
    appkey = "de504dc5763aeef9ff52"

    pusher = pusherclient.Pusher(appkey)

    pusher.connection.bind('pusher:connection_established', connect_handler)
    pusher.connect()
    insert_query_usd = "INSERT INTO btcstamp_usd (time, message) VALUES (?, ?)"
    #prepared_usd = session.prepare(insert_query_usd)
    insert_query_eur = "INSERT INTO btcstamp_eur (time, message) VALUES (?, ?)"
   # prepared_eur = session.prepare(insert_query_eur)

    while True:
        time.sleep(1)