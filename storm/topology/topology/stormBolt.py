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
            PRIMARY KEY (thekey, col1)
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
        self.unoccCabs = {}
        self.i = 0

    def process_tuple(self, tup):
        #log.debug(tup)

        result, = tup.values
        timestamp, data = result.split('@')
        listofstreets = data.split('#')

        for street in listofstreets:
            stID, direction, lane, carCount = street.split(',')
            #if self.i % 10000 is 0:
                #log.debug("Processed %d streets." % self.i)
            try:
                if not carCount or carCount is ' ':
                    carCount = '0'
                num = int(carCount)
                if num >= 2000:
                    self.i += 1
                    log.debug(stID + "," + timestamp + "," + direction + "," + lane + "," + str(num) + ", inserting %d into cassandra..." % self.i)
                #break
            except ValueError:
                log.debug('carCount is an empty string!   ---   ' + timestamp + ' ' + street)


            #if not carCount or carCount is '':    #
            #    carCount = '0'
            #log.debug(timestamp + ' - ' + carCount)
            #if int(carCount) > 2000:
            #    log.debug(stID + "," + timestamp + "," + direction + "," + lane + "," + carCount + ", inserting %d into cassandra..." % self.i)
            #    #session.execute(query, dict(key = stID , a = timestamp, b = carCount))
            #    self.i += 1

    #def process_tick(self):
    #    cur_cabs = self.unoccCabs
    #    colDict = {}
    #    for key, val in cur_cabs.iteritems():
    #        colDict['c:' + key] = json.dumps(val) # Add currently available cabs to HBase
    #    #minuteTbl.put('StormData', colDict)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/TestData.log',
        filemode='a',
    )

    firstBolt().run()

