# -*- coding: utf-8 -*-
'''
The script reads in a text file (header.csv) and writes the
stid - (stnames) table to cassandra.
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
    KEYSPACE = "station_header"

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
                leng text,
                beg text,
                end text,
                rdn text,
                spd text,
                PRIMARY KEY (stid)
            )
            """)

    query = SimpleStatement("""
        INSERT INTO header (stid, leng, beg, end, rdn, spd)
        VALUES (%(stid)s, %(leng)s, %(beg)s, %(end)s, %(rdn)s, %(spd)s)
        """, consistency_level=ConsistencyLevel.ONE)

    nol = sum(1 for line in open('header.csv'))
    idf = open('header.csv')
    bar = progressbar.ProgressBar(maxval=nol,
        widgets=[progressbar.Bar('=', '[', ']'),
        ' ',
        progressbar.Percentage()])

    print "Intaking header.csv..."
    bar.start()
    cnt = 0
    for line in idf:
        list_of_fields = line.split(',')

        session.execute(query, dict(stid=list_of_fields[0],
                        leng=list_of_fields[1],
                        beg=list_of_fields[2],
                        end=list_of_fields[3],
                        rdn=list_of_fields[4],
                        spd=list_of_fields[5]))

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