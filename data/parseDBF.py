'''
This script parses dbf database file.
The New York State Department of Transportation stores the (latitude, longitude)
of the stations in shapefile. Use this scripe to extract the geolocation info.
Note: among ~50,000 strees, only ~20,000 have (latitude, longitude)
'''

from dbfpy import dbf    # download from http://dbfpy.sourceforge.net/
import progressbar

db = dbf.Dbf("TDV_Continous_Count_2013.dbf")
of = open('cc.csv', 'w')
bar = progressbar.ProgressBar(maxval=len(db),
    widgets=[progressbar.Bar('=', '[', ']'), ' ',
    progressbar.Percentage()])
bar.start()
cnt = 0
for rec in db:
    ids = rec["RC_ID"].split('_')
    stid = ''.join(ids)
    lat = str(rec["LATITUDE"])
    lon = str(rec["LONGITUDE"])
    beg = rec["BEGINDESC"]
    end = rec["ENDDESC_1"]
    of.write(stid + ',' + lat + ',' + lon + ',' + beg + ',' + end + '\r\n')
    cnt += 1
    bar.update(cnt)

bar.finish()
of.close()

db = dbf.Dbf("TDV_Short_Count_2013.dbf")
of = open('sc.csv', 'w')
bar = progressbar.ProgressBar(maxval=len(db),
    widgets=[progressbar.Bar('=', '[', ']'), ' ',
    progressbar.Percentage()])
bar.start()
cnt = 0
for rec in db:
    ids = rec["RC_ID"].split('_')
    stid = ''.join(ids)
    lat = str(rec["LATITUDE"])
    lon = str(rec["LONGITUDE"])
    beg = rec["BEGINDESC"]
    end = rec["ENDDESC"]
    of.write(stid + ',' + lat + ',' + lon + ',' + beg + ',' + end + '\r\n')
    cnt += 1
    bar.update(cnt)

bar.finish()
of.close()
