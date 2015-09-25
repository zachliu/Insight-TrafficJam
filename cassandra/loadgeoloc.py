# -*- coding: utf-8 -*-
'''
The script reads in a text file (header.csv) and writes the
stid - stname table to cassandra.
'''
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import time
import progressbar


def main():

    t = time.time()

    cluster = Cluster(['54.175.15.242'])
    session = cluster.connect()
    KEYSPACE = "station_geoloc"

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
            CREATE TABLE header (
                stid text,
                lat text,
                lon text,
                beg text,
                end text,
                PRIMARY KEY (stid)
            )
            """)

    query = SimpleStatement("""
        INSERT INTO header (stid, lat, lon, beg, end)
        VALUES (%(stid)s, %(lat)s, %(lon)s, %(beg)s, %(end)s)
        """, consistency_level=ConsistencyLevel.ONE)

    nol = sum(1 for line in open('geoloc.csv'))
    idf = open('geoloc.csv')
    bar = progressbar.ProgressBar(maxval=nol,
        widgets=[progressbar.Bar('=', '[', ']'),
        ' ',
        progressbar.Percentage()])

    print "Intaking geoloc.csv..."
    bar.start()
    cnt = 0
    for line in idf:
        list_of_fields = line.split(',')

        session.execute(query, dict(stid=list_of_fields[0],
                        lat=list_of_fields[1],
                        lon=list_of_fields[2],
                        beg=list_of_fields[3],
                        end=list_of_fields[4]))

        cnt += 1
        bar.update(cnt)
    bar.finish()
    idf.close()

    elapsed = time.time() - t
    if elapsed < 60:
        print "Process took %.2f seconds!" % elapsed
    else:
        elapsed = elapsed / 60
        print "Process took %.2f mins!" % elapsed

if __name__ == "__main__":
    main()