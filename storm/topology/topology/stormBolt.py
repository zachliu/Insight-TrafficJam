# This program creates a storm bolt that transfer the incoming tuples into Cassandra
# which are then shown on UI. The Cassandra table is refreshed every 2.5 seconds using a tick tuple.

import logging
import time
import datetime
from pyleus.storm import SimpleBolt

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['54.175.15.242'])
#cluster = Cluster(['54.174.177.48'])
session = cluster.connect()
KEYSPACE = "keyspace_realtime"

log = logging.getLogger('TrafficJam')

rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
if KEYSPACE in [row[0] for row in rows]:
    session.set_keyspace(KEYSPACE)
else:
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
        """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE mytable (
            thekey text,
            col1 text,
            col2 text,
            PRIMARY KEY (thekey)
        )
        """)

query = SimpleStatement("""
    INSERT INTO mytable (thekey, col1, col2)
    VALUES (%(key)s, %(a)s, %(b)s)
    """, consistency_level=ConsistencyLevel.ONE)

insert = session.prepare("""
    INSERT INTO mytable (thekey, col1, col2)
    VALUES (?, ?, ?)
    """)


class firstBolt(SimpleBolt):

    OUTPUT_FIELDS = ['data']

    def initialize(self):
        self.busyStreets = {}
        self.i = 0

    def process_tuple(self, tup):
        result, = tup.values
        timestamp, data = result.split('@')
        listofstreets = data.split('#')
        err = 0
        for street in listofstreets:
            stID, direction, lane, carCount = street.split(',')
            try:
                if not carCount or carCount is ' ':
                    carCount = '0'
                num = int(carCount)
                self.i += 1
                self.busyStreets[stID] = {'ts': timestamp, 'cc': str(num)}

            except ValueError:
                err += 1
                log.info('carCount is an empty string!   ---   ' + timestamp + ' ' + street)

    def process_tick(self):
        cur_streets = self.busyStreets
        start = time.time()
        cnt = 0
        for stID, val in cur_streets.iteritems():
            session.execute(query, dict(key=stID, a=val['ts'], b=val['cc']))
            cnt += 1
        if cnt >= 1500:
            dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log.info(dt + ': It took ' + "{:.2f}".format(time.time() - start) + ' sec to insert ' + str(cnt) + ' entries into Cassandra')
        self.busyStreets = {}

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/TrafficJam.log',
        filemode='a',
    )
    firstBolt().run()

