# -*- coding: utf-8 -*-
'''
The script tests queries to cassandra.
'''

from cassandra.cluster import Cluster
import time


def main():

    cluster = Cluster(['54.175.15.242'])
    session = cluster.connect()
    session.set_keyspace("station_header")

    t = time.time()
    for i in range(1):    # about 2ms per query
        stid = "044254"
        rows = session.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
        print rows[0].stid

    session.set_keyspace("configurations")
    rows = session.execute("SELECT * FROM conf WHERE pid = '%s'" % 'storm')
    print rows[0].cc

    elapsed = time.time() - t
    if elapsed < 60:
        print "Process took %.2f seconds!" % elapsed
    else:
        elapsed = elapsed / 60
        print "Process took %.2f mins!" % elapsed

if __name__ == "__main__":
    main()