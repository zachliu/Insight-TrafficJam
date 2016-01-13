# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
'''
The script reads in a text file (header.csv) and writes the
stid - (latitude, longitude) table to cassandra.
'''
import MySQLdb as mdb
import time
import warnings
import progressbar


def main():

    t = time.time()

    # Open database connection
    db = mdb.connect("localhost", "root", "", "traffic")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Drop table if it already exist using execute() method.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cursor.execute("DROP TABLE IF EXISTS roadloc")

    print "creating table..."
    sql = """
        CREATE TABLE roadloc (
            rcid CHAR(6) NOT NULL,
            coord text,
            PRIMARY KEY (rcid)
        )
        """

    cursor.execute(sql)

    nol = sum(1 for line in open('roadloc.csv'))
    idf = open('roadloc.csv')
    bar = progressbar.ProgressBar(maxval=nol,
        widgets=[progressbar.Bar('=', '[', ']'),
        ' ',
        progressbar.Percentage()])

    print "Intaking roadloc.csv..."
    bar.start()
    cnt = 0
    for line in idf:
        list_of_fields = line.rstrip('\n').split('@')
        if len(list_of_fields) == 2:
            # Prepare SQL query to INSERT a record into the database.
            sql = "INSERT INTO roadloc(rcid, coord) VALUES ('%s', '%s')" % \
                   (list_of_fields[0], list_of_fields[1])
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
        print "Processing %d entries took %.2f seconds!" % (cnt, elapsed)
    else:
        elapsed = elapsed / 60
        print "Processing %d entries took %.2f mins!" % (cnt, elapsed)

    # disconnect from server
    db.close()

if __name__ == "__main__":
    main()