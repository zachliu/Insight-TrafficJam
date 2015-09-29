from osgeo import ogr
from osgeo import osr
import shapefile as shp
#import matplotlib.pyplot as plt
import time
import progressbar

t = time.time()

source = osr.SpatialReference()
source.ImportFromEPSG(26918)

target = osr.SpatialReference()
target.ImportFromEPSG(4326)

transform = osr.CoordinateTransformation(source, target)

sf = shp.Reader("TDV_AADT_2013")

bar = progressbar.ProgressBar(maxval=len(sf.records()),
    widgets=[progressbar.Bar('=', '[', ']'), ' ',
    progressbar.Percentage()])

of = open('roadloc.csv', 'w')

#plt.figure()
bar.start()
cnt = 0
for shape in sf.shapeRecords():
    lonlat = []
    stid = ''.join(shape.record[0].split('_'))
    for i in shape.shape.points[:]:
        point = ogr.CreateGeometryFromWkt("POINT (%s %s)" % (str(i[0]), str(i[1])))
        point.Transform(transform)
        p = point.GetPoint()
        lonlat.append(str(p[0]) + ',' + str(p[1]))
    cat = ';'.join(lonlat)
    if cat:
        of.write(stid + '@' + cat + '\r\n')
    #plt.plot(x,y)
    cnt += 1
    #if cnt is 5:
    #    break
    bar.update(cnt)
bar.finish() 
of.close()
elapsed = time.time() - t
if elapsed < 60:
    print "Process took %.2f seconds!" % elapsed
else:
    elapsed = elapsed / 60
    print "Process took %.2f mins!" % elapsed
    
#plt.show()



