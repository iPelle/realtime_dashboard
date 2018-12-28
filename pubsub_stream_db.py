import sqlite3
import json
import time
import random
import pandas as pd

import multiprocessing
import base64
from config import *

from google.cloud import pubsub_v1

table_name = "streaming_data"
db_name = "streaming_db"

conn = None
c = None

first_data_received = 0
show_received_messages = 0

def initiate_db(db_name, table_name):

	conn = sqlite3.connect(db_name)
	c = conn.cursor()
	sql_drop_table = "DROP TABLE IF EXISTS {};".format(table_name)
	c.execute(sql_drop_table)
	conn.commit()
	conn.close()
	return None

def create_table(data_dict):
	conn = sqlite3.connect(db_name)
	c = conn.cursor()
	#create a table with only REAL-columns. No index
	create_columns = ' REAL, '.join(data_dict.keys()) + ' REAL'
	create_table_query = 'CREATE TABLE IF NOT EXISTS  %s (%s) ' % (table_name, create_columns)
	c.execute(create_table_query)
	conn.commit()
	conn.close()
	return None


def create_table_with_id():
	#TODO
	#Like function above, but handling id's, as non numeric strings. Non REAL.
	return None
	
def insert_in_table(data_dict):
	conn = sqlite3.connect(db_name)
	c = conn.cursor()
	columns = ', '.join(data_dict.keys())
	placeholders = ':'+', :'.join(data_dict.keys())
	query = 'INSERT INTO streaming_data (%s) VALUES (%s)' % (columns, placeholders)
	c.execute(query, data_dict)
	conn.commit()
	conn.close()


############################

def getJsonData():
    
    # Simulate data as dict
    json_dict ={
    "time":time.time(),
    "OilTemperature":random.randrange(180,230),
    "IntakeTemperature": random.randrange(95,115),
    "CoolantTemperature": random.randrange(170,220)}

    # Convert and return JSON
    data_json = json.dumps(json_dict)
    return data_json
    
def wait_for_data(param):
    print("Waiting for first data...")
    while first_data_received == 0:
        time.sleep(1)
        return None
    print("First data received")
    print("Streaming data...")
    return None
	
def receive_messages(project_ID, subscription_name):
    """Receives messages from a pull subscription."""
    # [START pubsub_subscriber_async_pull]
    # [START pubsub_quickstart_subscriber]
    # project           = "Your Google Cloud Project ID"
    # subscription_name = "Your Pubsub subscription name"
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project_ID, subscription_name)

    def callback(message):
        
        if show_received_messages == 1: print('Received message: {}'.format(message))
        collect_data(message.data)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    # The subscriber is non-blocking, so we must keep the main thread from
    # exiting to allow it to process messages in the background.
    print('Listening for messages on {}'.format(subscription_path))

def decode_to_dict(data):
	stream = data.decode('utf-8')
	data_raw = json.loads(stream)
	return data_raw


def collect_data(data):
    global first_data_received
    new_dict = decode_to_dict(data)
    if first_data_received == 0:
        create_table(new_dict)
    insert_in_table(new_dict)
    first_data_received = 1
    return None

if __name__ == '__main__':
    initiate_db(db_name, table_name)
    receive_messages(project_ID, subscription_name)
    wait_for_data(first_data_received)
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    while True:
        time.sleep(5)
        df = pd.read_sql("SELECT * FROM streaming_data", conn)
        print(df)