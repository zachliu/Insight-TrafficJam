# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
'''
The script creates a keyspace and table in cassandra to store
conf from UI
The conf are used by storm/spark
'''
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import time


def main():

    t = time.time()

    cluster = Cluster(['54.175.15.242'])
    session = cluster.connect()
    KEYSPACE = "configurations"

    #log = logging.getLogger('TestData')

    print "creating keyspace..."

    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        print "There is an existing keyspace..."
        print "setting keyspace..."
        session.set_keyspace(KEYSPACE)
    else:
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = { 'class': 'SimpleStrategy',
            'replication_factor': '3' }
            """ % KEYSPACE)

        print "setting keyspace..."
        session.set_keyspace(KEYSPACE)

        print "creating table..."
        session.execute("""
            CREATE TABLE conf (
                pid text,
                cc text,
                PRIMARY KEY (pid)
            )
            """)

    query = SimpleStatement("""
        INSERT INTO conf (pid, cc)
        VALUES (%(pid)s, %(cc)s)
        """, consistency_level=ConsistencyLevel.ONE)

    print "Writing confs..."

    session.execute(query, dict(pid='storm', cc='6000'))

    elapsed = time.time() - t
    if elapsed < 60:
        print "Process took %.2f seconds!" % elapsed
    else:
        elapsed = elapsed / 60
        print "Process took %.2f mins!" % elapsed

if __name__ == "__main__":
    main()