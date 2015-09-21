# Kafka producer that reads the input data in a loop in order to simulate real time events
import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime
import time
import logging

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.DEBUG
)

#kafka = KafkaClient("54.209.17.13:9092")
kafka = KafkaClient("54.175.15.242:9092")

#source_file = '/home/zexi/data_sample.txt'
source_file = '/home/zexi/test_traffic_data.txt'

def genData(topic):
    producer = KeyedProducer(kafka)
    while True:
        ifile = open(source_file)
        #with open(source_file) as f:
        for line in ifile:
            key = line.split(",")[0]
            producer.send(topic, key, line.rstrip())
            time.sleep(0.1)  # Creating some delay to allow proper rendering of the cab locations on the map

        #source_file.close()
        ifile.close()

#genData("TestData")
genData("my-topic")
