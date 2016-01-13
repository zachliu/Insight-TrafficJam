# -*- coding: utf-8 -*-
'''
The script reads in a text file (header.csv) and writes the
stid - (stnames) table to cassandra.
'''
import MySQLdb as mdb
import time
import warnings
import progressbar
import glob


def main():

    t = time.time()

    # Open database connection
    db = mdb.connect("localhost", "root", "", "traffic")

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
             ts DATETIME NOT NULL,
             dir CHAR(1),
             lane CHAR(1),
             count INT(10) UNSIGNED NOT NULL,
             PRIMARY KEY (rcid)
             )"""

    cursor.execute(sql)

    print "Ingest traffic data as realtime..."

    csvfiles = sorted(glob.glob("*.csv"))

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
            if len(list_of_fields) == 5 and \
               len(list_of_fields[0]) != 0 and \
               len(list_of_fields[1]) != 0 and \
               len(list_of_fields[2]) != 0 and \
               len(list_of_fields[3]) != 0 and \
               len(list_of_fields[4]) != 0:
                date, hhmmss = list_of_fields[1].split(' ')
                mm, dd, yyyy = date.split('/')
                timestamp = yyyy + "-" + mm + "-" + dd + " " + hhmmss
                # Prepare SQL query to INSERT a record into the database.
                sql = "INSERT INTO realtime(rcid, ts, dir, lane, count) VALUES ('%s', '%s', '%s', '%s', '%s')" % \
                       (list_of_fields[0], timestamp, list_of_fields[2], list_of_fields[3], list_of_fields[4])
                try:
                    # Execute the SQL command
                    cursor.execute(sql)
                    # Commit your changes in the database
                    db.commit()
                except:
                    # Rollback in case there is any error
                    db.rollback()

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
    db.close()

if __name__ == "__main__":
    main()