# Kafka producer that reads the input data in a loop in order to simulate real time events
import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from kafka.producer import SimpleProducer
from datetime import datetime
import time
import progressbar
#import logging

#logging.basicConfig(
#    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
#    level=logging.DEBUG
#)

#kafka = KafkaClient("54.209.17.13:9092")
#kafka = KafkaClient("54.175.15.242:9092")
kafka = KafkaClient("localhost:9092")

#source_file = '/home/zexi/data_sample.txt'
#source_file = '/home/ubuntu/test_traffic_data.txt'
source_file = '/home/ubuntu/VOL_2012_rev.csv'

bar = progressbar.ProgressBar(maxval=16709, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])

def send_message(topic):
    producer = SimpleProducer(kafka)
    k = 1
    bksize = 100 # bulk size
    c = 0
    while k is 1:
        ifile = open(source_file)
        msg_bulk = []
        bar.start()
        for line in ifile:
            msg_bulk.append(line)
            if len(msg_bulk) >= bksize:
                _send_bulk(producer, topic, msg_bulk)
                c += bksize
                bar.update(c+1)
                msg_bulk = []
                #raw_input("Press Enter to send the next bulk...")
                time.sleep(0.1)  # Creating some delay to allow proper rendering of the locations on the map
                #k = 0
                #break
        bar.finish()
        k = 0
        print "File completed!"
        ifile.close()


def _send_bulk(producer, topic, msg_bulk):
    #msg_list = map(str, msg_bulk)
    producer.send_messages(topic, *msg_bulk)

send_message("traffic_1")
