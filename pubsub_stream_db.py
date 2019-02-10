import sqlite3
import json
import time
import random
import pandas as pd
import os

import multiprocessing
import base64
from config import *

from google.cloud import pubsub_v1

table_name = "streaming_data"
db_name = "streaming_db"
cached_table_names = []

conn = None
c = None

first_data_received = 0
show_received_messages = 0

def initiate_db(db_name, table_name):
    try:
        os.remove(db_name)
        print("Database dropped")
    except: 
        print("No database to remove")
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    conn.close()
    return None

def create_table(data_dict):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    data_id = data_dict['id']
    id_as_table_name = data_id
    datetime = data_dict['datetime']
    data_dict = data_dict['data']
    create_columns = ' REAL, '.join(data_dict.keys()) + ' REAL'
    create_table_query = 'CREATE TABLE IF NOT EXISTS  %s ( id TEXT, datetime REAL, %s) ' % (id_as_table_name, create_columns)
    c.execute(create_table_query)
    print("Created table {}".format(data_id))
    conn.commit()
    conn.close()
    return None

def table_id_exists(table_id):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    c.execute(query)
    tuple_of_tables = list(c)
    list_of_tables = [i[0] for i in tuple_of_tables]
    #print("Table ID is: {}".format(table_id))
    #print("List of tables is: {}".format(list_of_tables))
    if table_id in list_of_tables:
        #print("Table ID in list_of_tables")
        return True
    else:
        #print("Table ID NOT in list_of_tables")
        return False

def insert_in_table(data_dict):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    if not table_id_exists(data_dict['id']):
        #Create table in db if not exists
        create_table(data_dict)
    # When table has been created, add data to table
    data_id = data_dict['id']
    id_as_table_name = data_id
    datetime = data_dict['datetime']
    data_dict = data_dict['data']
    columns = ', '.join(data_dict.keys())
    placeholders = ':'+', :'.join(data_dict.keys())
    query = 'INSERT INTO {} (id, datetime, %s) VALUES ("{}", {}, %s)'.format(id_as_table_name,data_id,datetime) % (columns, placeholders)

    c.execute(query, data_dict)
    conn.commit()
    conn.close()


############################

    
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
#TODO: Remove table creation from "Collect data", add check in "Insert in table", to check if table exists. Otherwise create table

def collect_data(data):
    global first_data_received
    new_dict = decode_to_dict(data)
    #if first_data_received == 0:
        #create_table(new_dict)
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
        #df = pd.read_sql("SELECT * FROM streaming_data", conn)
        #print(df)