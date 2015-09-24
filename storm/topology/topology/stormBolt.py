# This program creates a storm bolt that filters the incoming tuples based on occupancy
# It stores the unoccupied cab details into HBase which are then shown on UI. The HBase
# table is refreshed every 5 seconds using a tick tuple.
import logging
from pyleus.storm import SimpleBolt
#import happybase
#import json

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

#connection = happybase.Connection('54.67.126.144')
#minuteTbl = connection.table('avlbl_Cabs')

cluster = Cluster(['54.175.15.242'])
session = cluster.connect()
KEYSPACE = "keyspace_realtime"

log = logging.getLogger('TestData')

log.debug("creating keyspace...")

rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
if KEYSPACE in [row[0] for row in rows]:
    log.debug("There is an existing keyspace...")
    log.debug("setting keyspace...")
    session.set_keyspace(KEYSPACE)
else:
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
        """ % KEYSPACE)

    log.debug("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    log.debug("creating table...")
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

prepared = session.prepare("""
    INSERT INTO mytable (thekey, col1, col2)
    VALUES (?, ?, ?)
    """)

class firstBolt(SimpleBolt):

    OUTPUT_FIELDS = ['data']

    def initialize(self):
        self.busyStreets = {}
        self.i = 0
        cluster = Cluster(['54.175.15.242'])
        session = cluster.connect()
        session.set_keyspace("configurations")
        rows = session.execute("SELECT * FROM conf WHERE pid = '%s'" % 'storm')
        self.cc = rows[0].cc
        self.new = False

    def process_tuple(self, tup):
        #log.debug(tup)

        result, = tup.values
        timestamp, data = result.split('@')
        listofstreets = data.split('#')

        for street in listofstreets:
            stID, direction, lane, carCount = street.split(',')
            try:
                if not carCount or carCount is ' ':
                    carCount = '0'
                num = int(carCount)
                if num >= 2000:  # int(self.cc):
                    self.i += 1
                    log.debug(stID + "," + timestamp + "," + direction + "," + lane + "," + str(num) + ", inserting %d into table..." % self.i)
                    self.busyStreets[stID] = {'ts':timestamp, 'cc':str(num)}
                    self.new = True
                else:
                    if num <= 20:
                        if stID in self.busyStreets.keys():
                            del self.busyStreets[stID]
                            session.execute("DELETE FROM mytable WHERE thekey = '%s'" % stID)
                            log.debug(stID + ' has been removed from table.')
                            self.new = True
            except ValueError:
                log.debug('carCount is an empty string!   ---   ' + timestamp + ' ' + street)

    def process_tick(self):
        cur_streets = self.busyStreets
        if self.new is True:
            for stID, val in cur_streets.iteritems():
                session.execute(query, dict(key = stID , a = val['ts'], b = val['cc']))
                log.debug(stID + ' has been written into cassandra.')
            self.new = False

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/TestData.log',
        filemode='a',
    )

    firstBolt().run()

