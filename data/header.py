# -*- coding: utf-8 -*-
'''
This script processes the station header file extracting
street id, length of the stree section, beginning intersection name,
end intersection name, the street name, and the speed limit.
'''
#import sys
#import os
import progressbar


def main():

    fname = 'OFF_NETWORK_HEADER.csv'
    f = open(fname)
    of = open('header.csv', 'w')

    next(f)  # ignore the 1st row
    cnt = 0
    nol = sum(1 for line in open(fname))
    bar = progressbar.ProgressBar(maxval=nol,
        widgets=[progressbar.Bar('=', '[', ']'),
        ' ', progressbar.Percentage()])
    bar.start()
    for line in f:
        list_of_fields = line.split(',')

        RCID = list_of_fields[0] + list_of_fields[8]
        Length = list_of_fields[9]
        BegDesc = list_of_fields[11]
        EndDesc = list_of_fields[12]
        RdName = list_of_fields[18]
        Speed = list_of_fields[22]

        newlist = [RCID, Length, BegDesc, EndDesc, RdName, Speed]

        #print timestamp
        #continue

        newline = ','.join(newlist)

        #print newline.rstrip()
        #continue
        of.write(newline + '\r\n')

        cnt += 1
        bar.update(cnt + 1)

    bar.finish()
    of.close()
    f.close()

if __name__ == "__main__":
    main()
