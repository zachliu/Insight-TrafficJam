# Kafka producer that reads the input data in a loop in order to simulate real time events
from kafka import KafkaClient
from kafka.producer import SimpleProducer
#from datetime import datetime
import time
import progressbar
import glob

#kafka = KafkaClient("54.175.15.242:9092")
kafka = KafkaClient("localhost:9092")
csvfiles = sorted(glob.glob("/home/ubuntu/*.csv"))


def send_message(topic):
    producer = SimpleProducer(kafka)
    bksize = 10  # bulk size
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
                    time.sleep(1)
            bar.finish()
            print "File " + f + " completed!"
            ifile.close()


def _send_bulk(producer, topic, msg_bulk):
    producer.send_messages(topic, *msg_bulk)

send_message("traffic")
