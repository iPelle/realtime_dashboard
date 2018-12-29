import time
import random
import json
from google.cloud import pubsub #pip install google-cloud-pubsub
from config import *
from datetime import datetime

indicator = 1
counter = 0

def getJsonData():
    
    # Simulate data as dict
    json_dict ={
    "id":random.randrange(1000,9999),
    "datetime":datetime.now().timestamp(),
    "data":{
        "OilTemperature":random.randrange(180,230),
        "IntakeTemperature": random.randrange(95,115),
        "CoolantTemperature": random.randrange(170,220)
        }
    }

    # Convert and return JSON
    data_json = json.dumps(json_dict)
    return data_json

def send_data():
	global counter
	
	#Create publisher
	publisher = pubsub.PublisherClient()
	topic_path = publisher.topic_path(project, topic_name)
	print(topic_path)
	
	while indicator == 1:
		time.sleep(0.1)
		counter += 1
		print(counter)
		data = getJsonData()
		data = data.encode('utf-8')
		print(data)
		message_id = publisher.publish(topic_path, data=data)
		print('Message '+repr(message_id)+'published.') 

if __name__ == '__main__':

	#generate_data()
	send_data()