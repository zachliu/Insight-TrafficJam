# -*- coding: utf-8 -*-
'''
The script reads in a text file (header.csv) and writes the
stid - (stnames) table to cassandra.
'''
import MySQLdb as mdb
#import mysql.connector as mdb
import time
import warnings
import progressbar
import glob


def main():

    t = time.time()

    # Open database connection
    db = mdb.connect("localhost", "root", "", "traffic")
    #db = mdb.connect(user='root', password='', host='127.0.0.1', database='traffic')

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Drop table if it already exist using execute() method.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cursor.execute("DROP TABLE IF EXISTS realtime")

    # Create table as per requirement
    print "creating table..."
    sql = """CREATE TABLE realtime (
             rcid CHAR(6) NOT NULL,
             count INT(10) UNSIGNED NOT NULL,
             PRIMARY KEY (rcid)
             )"""

    cursor.execute(sql)

    print "Ingest traffic data as realtime..."

    csvfiles = sorted(glob.glob("*.csv"))

    road_average_count = {}

    for csv in csvfiles:
        nol = sum(1 for line in open(csv))
        idf = open(csv)
        next(idf)
        bar = progressbar.ProgressBar(maxval=nol,
            widgets=[progressbar.Bar('=', '[', ']'),
            ' ',
            progressbar.Percentage()])

        print "Intaking " + csv + "..."
        bar.start()
        cnt = 0
        for line in idf:
            list_of_fields = line.rstrip('\n').split(',')
            if len(list_of_fields) == 5:
                #date, hhmmss = list_of_fields[1].split(' ')
                #mm, dd, yyyy = date.split('/')
                #timestamp = yyyy + "-" + mm + "-" + dd + " " + hhmmss

                rcid = list_of_fields[0]
                carcount = list_of_fields[4]

                if rcid in road_average_count:
                    road_average_count[rcid] = (road_average_count[rcid][0] + int(carcount),
                                                road_average_count[rcid][1] + 1)
                else:
                    road_average_count[rcid] = (int(carcount), 1)
            cnt += 1
            bar.update(cnt)
        bar.finish()
        idf.close()
        print "Size of dict: %s" % len(road_average_count)

    bar = progressbar.ProgressBar(maxval=len(road_average_count),
        widgets=[progressbar.Bar('=', '[', ']'),
        ' ',
        progressbar.Percentage()])

    print "Intaking road_average_count..."
    bar.start()
    cnt = 0
    for key in road_average_count:
        # Prepare SQL query to INSERT a record into the database.
        sql = """INSERT INTO realtime(rcid, count)
                 VALUES ('%s', '%s')""" % \
                 (key, road_average_count[key][0] / road_average_count[key][1])
        #try:
            # Execute the SQL command
        cursor.execute(sql)
            # Commit your changes in the database
        db.commit()
        #except:
            # Rollback in case there is any error
            #db.rollback()
        cnt += 1
        bar.update(cnt)
    bar.finish()
    idf.close()

    elapsed = time.time() - t
    if elapsed < 60:
        print "Process %d took %.2f seconds!" % (cnt, elapsed)
    else:
        elapsed = elapsed / 60
        print "Process %d took %.2f mins!" % (cnt, elapsed)

    # disconnect from server
    #cursor.close()
    db.close()

if __name__ == "__main__":
    main()