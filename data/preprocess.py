# -*- coding: utf-8 -*-
'''
This script sorts the raw data by their timestamp and aggregates all entries
that have the same timestamp.
'''
import sys
#import os
import time
import progressbar


def main(args):

    t = time.time()

    # open header.csv and create a dict
    #id_dict = {}
    #nol = sum(1 for line in open('header.csv'))
    #idf = open('header.csv')
    #bar = progressbar.ProgressBar(maxval=nol,
    #    widgets=[progressbar.Bar('=', '[', ']'), ' ',
    #    progressbar.Percentage()])
    #print "Intaking header.csv..."
    #bar.start()
    #cnt = 0
    #for line in idf:
    #    list_of_fields = line.split(',')
    #    key = list_of_fields[0]
    #    if key not in id_dict.keys():
    #        id_dict[key] = line.rstrip()
    #    cnt += 1
    #    bar.update(cnt)
    #bar.finish()

    fileName = args[1] + '.csv'

    nol = sum(1 for line in open(fileName))
    bar = progressbar.ProgressBar(maxval=nol,
            widgets=[progressbar.Bar('=', '[', ']'), ' ',
            progressbar.Percentage()])

    f = open(fileName)

    ts_dict = {}    # Dictionary that will hold all the uniques words as keys
                    # and the number of their occurances as values

    print "Processing " + fileName + " ..."
    bar.start()
    next(f)  # ignore the 1st row
    cnt = 0
    for line in f:
        list_of_fields = line.split(',')
        timestamp = list_of_fields[1]
        #print timestamp
        #continue

        del list_of_fields[1]

        newline = ','.join(list_of_fields)

        #print newline.rstrip()
        #continue
        if timestamp in ts_dict.keys():
            ts_dict[timestamp].append(newline.rstrip())
        else:
            ts_dict[timestamp] = [newline.rstrip()]

        cnt += 1
        bar.update(cnt)

    bar.finish()

    # Sort the dict based on timestamp
    sorted_ts_dict = sorted(ts_dict)

    cnt = 1
    try:
        of = open(fileName.split('.')[0] + '_rev.csv', 'w')
    except IOError:
        print "Could not open file! Please close Excel!"

    #print fileName.split('.')[0] + '_rev.csv'

    print "Saving " + fileName.split('.')[0] + '_rev.csv' + " ..."
    bar = progressbar.ProgressBar(maxval=len(sorted_ts_dict),
        widgets=[progressbar.Bar('=', '[', ']'), ' ',
        progressbar.Percentage()])
    bar.start()
    cnt = 0
    for key in sorted_ts_dict:
        myList = ts_dict[key]

        #for idx, item in enumerate(myList):
        #    data = item.split(',')
        #    stid = data[0]
        #    del data[0]
        #    if stid in id_dict.keys():
        #        infoList = id_dict[stid]
        #    else:
        #        infoList = ''
        #        print 'Street id ' + stid + ' not found!'
        #    myList[idx] = ','.join(data) + ',' + infoList

        cat = '#'.join(myList)
        of.write(key + '@' + cat + '\r\n')
        cnt += 1
        bar.update(cnt)
    bar.finish()

    of.close()
    f.close()
    elapsed = time.time() - t
    if elapsed < 60:
        print "Process took %.2f seconds!" % elapsed
    else:
        elapsed = elapsed / 60
        print "Process took %.2f mins!" % elapsed

if __name__ == "__main__":
    main(sys.argv)
