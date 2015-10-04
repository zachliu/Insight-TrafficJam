# Kafka producer that reads the input data in a loop in order to simulate real time events
#import os
#import sys
from kafka import KafkaClient
from kafka.producer import SimpleProducer
#from datetime import datetime
import time
import progressbar
#import logging
import glob

#logging.basicConfig(
#    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
#    level=logging.DEBUG
#)

#kafka = KafkaClient("54.209.17.13:9092")
#kafka = KafkaClient("54.175.15.242:9092")
kafka = KafkaClient("localhost:9092")

#source_file = '/home/zexi/data_sample.txt'
#source_file = '/home/ubuntu/test_traffic_data.txt'
#source_file = '/home/ubuntu/VOL_2001_rev.csv'
#source_file = '/home/ubuntu/VOL_2002_rev.csv'
#source_file = '/home/ubuntu/VOL_2003_rev.csv'
#source_file = '/home/ubuntu/VOL_2004_rev.csv'
#source_file = '/home/ubuntu/VOL_2005_rev.csv'
#source_file = '/home/ubuntu/VOL_2006_rev.csv'
#source_file = '/home/ubuntu/VOL_2007_rev.csv'
#source_file = '/home/ubuntu/VOL_2008_rev.csv'
#source_file = '/home/ubuntu/VOL_2009_rev.csv'
#source_file = '/home/ubuntu/VOL_2010_rev.csv'
#source_file = '/home/ubuntu/VOL_2011_rev.csv'
#source_file = '/home/ubuntu/VOL_2012_rev.csv'


csvfiles = sorted(glob.glob("/home/ubuntu/*.csv"))


def send_message(topic):
    producer = SimpleProducer(kafka)
    bksize = 100  # bulk size
    while True:
        for f in csvfiles:
            nol = sum(1 for line in open(f))
            bar = progressbar.ProgressBar(maxval=nol,
                widgets=[progressbar.Bar('=', '[', ']'), ' ',
                progressbar.Percentage()])
            c = 0
            ifile = open(f)
            msg_bulk = []
            bar.start()
            for line in ifile:
                msg_bulk.append(line)
                if len(msg_bulk) >= bksize:
                    _send_bulk(producer, topic, msg_bulk)
                    c += bksize
                    bar.update(c + 1)
                    msg_bulk = []
                    #raw_input("Press Enter to send the next bulk...")
                    # Creating some delay to allow proper rendering of the locations on the map
                    #time.sleep(1)
                    time.sleep(0.1)
            bar.finish()
            print "File " + f + " completed!"
            ifile.close()


def _send_bulk(producer, topic, msg_bulk):
    #msg_list = map(str, msg_bulk)
    producer.send_messages(topic, *msg_bulk)

send_message("traffic")
