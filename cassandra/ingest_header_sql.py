# -*- coding: utf-8 -*-
'''
The script reads in a text file (header.csv) and writes the
stid - (stnames) table to cassandra.
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
        cursor.execute("DROP TABLE IF EXISTS header")

    print "creating table..."
    sql = """
        CREATE TABLE header (
            rcid CHAR(6) NOT NULL,
            len INT(10) UNSIGNED,
            beg text,
            end text,
            rdn text,
            spd INT(10) UNSIGNED,
            PRIMARY KEY (rcid)
        )
        """

    cursor.execute(sql)

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
        list_of_fields = line.rstrip('\n').split(',')
        if len(list_of_fields) == 6:
            # Prepare SQL query to INSERT a record into the database.
            sql = "INSERT INTO header(rcid, len, beg, end, rdn, spd) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % \
                   (list_of_fields[0], list_of_fields[1], list_of_fields[2], list_of_fields[3], list_of_fields[4], list_of_fields[5])
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