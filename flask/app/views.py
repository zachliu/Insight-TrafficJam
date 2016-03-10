from app import app
#from cassandra.cluster import Cluster
import flask
from flask import jsonify, render_template, make_response, request
import json
from json import dumps
import geojson
from geojson import Feature, MultiLineString, FeatureCollection
import time
import sys
import datetime
import MySQLdb as mdb

#cluster = Cluster(['54.175.15.242'])
##cluster = Cluster(['54.174.177.48'])
#session_real = cluster.connect()
#session_batch = cluster.connect()
#session_findname = cluster.connect()
#session_findloc = cluster.connect()

#session_findloc.set_keyspace("road_geoloc")
#start = time.time()
#lookup = {}
#cass = session_findloc.execute("SELECT * FROM header")

#for e in cass:
    #lookup[str(e.stid)] = str(e.coord)
#print time.time() - start

zoom = 7

# -------------------------------------------------------------------------------------------------
db_lookup = mdb.connect("localhost", "root", "", "traffic")    # Open database connection
cursor = db_lookup.cursor()    # prepare a cursor object using cursor() method
start = time.time()
roadloc = {}
header = {}
cursor.execute("SELECT * FROM roadloc")
data = cursor.fetchall()
for e in data:
    roadloc[str(e[0])] = str(e[1])
cursor.execute("SELECT * FROM header")
data = cursor.fetchall()
for e in data:
    header[str(e[0])] = (e[1], str(e[2]), str(e[3]), str(e[4]), e[5])
print time.time() - start
db_lookup.close()
# -------------------------------------------------------------------------------------------------

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/maps')
def maps():
    return render_template('index.html')


@app.route('/slideshare')
def aboutme():
    co = request.cookies["session_id"]
    ip = request.remote_addr + '.' + co    # get the user ip and cookie
    dt = str(datetime.datetime.now())
    Printer(dt + ' ' + str(len(iptable)) + ' ip: ' + ip + ' ' + 'slideshare')
    return render_template('slideshare.html')

@app.route('/demo')
def demo():
    co = request.cookies["session_id"]
    ip = request.remote_addr + '.' + co    # get the user ip and cookie
    dt = str(datetime.datetime.now())
    Printer(dt + ' ' + str(len(iptable)) + ' ip: ' + ip + ' ' + 'video')
    return render_template('demo.html')


Colors = [
    "#20CA20",    # green
    "#FF5800",    # purple
    "#F50000",    # red
    "#941313"     # dark red
]


def chooseColor(cc):
    cc = int(cc)
    if cc > 2000:
        return Colors[3]
    elif cc > 1000:
        return Colors[2]
    elif cc > 500:
        return Colors[1]
    else:
        return Colors[0]


class Printer():
    """
    Print things to stdout on one line dynamically
    """
    def __init__(self, data):
        sys.stdout.write(data.__str__() + '\r')
        #sys.stdout.write("\r\x1b[K" + data.__str__())
        sys.stdout.flush()


iptable = {}    # create for each user
counter = 0


@app.route('/realtime_roads')
def realtime_roads():
    co = request.cookies["session_id"]
    ip = request.remote_addr + '.' + co    # get the user ip and cookie
    if ip not in iptable:
        iptable[ip] = {}
    global counter
    counter += 1
    rows = []
    #session_real.set_keyspace("keyspace_realtime")
    #rows = session_real.execute("SELECT * FROM mytable")
    # ---------------------------------------------------------------------------------------------
    db = mdb.connect("localhost", "root", "", "traffic")    # Open database connection
    cursor = db.cursor()    # prepare a cursor object using cursor() method
    cursor.execute("SELECT * FROM realtime")
    rows = cursor.fetchall()
    db.close()
    # ---------------------------------------------------------------------------------------------
    roads = []
    total = 0

    for row in rows:
        stid = str(row[0].split('\'')[0])
        cc = str(row[1])
        if len(roads) >= 1500:
            break
        if (stid not in iptable[ip]) or ((stid in iptable[ip]) and (iptable[ip][stid] is not chooseColor(cc))):
            iptable[ip][stid] = chooseColor(cc)
            start = time.time()
            coords = ''
            if stid in roadloc:
                coords = roadloc[stid]
            total += time.time() - start

            if len(coords) is not 0:
                coord = []
                listofpairs = coords.split(';')
                for entry in listofpairs:
                    coord.append(entry.split(','))
                roads.append({'name': stid, 'carcount': cc, 'coord': coord})
    dt = str(datetime.datetime.now())
    Printer(dt + ' ' + str(len(iptable)) + ' ip: ' + ip + ' ' + str(len(roads)))
    #Printer(dt + ' ' + str(len(iptable)) + ' ip: ' + ip + ' ' + 'main page')
    return jsonify(roads=roads)


#session_all = cluster.connect()


@app.route('/zoom_changed')
def add_numbers():
    zoom = request.args.get('zoom', 0, type=int)
    #Printer(" === " + str(zoom) + " === ")
    return jsonify(result=(zoom) * 10)


@app.route('/all_roads')
def all_roads():
    rows = []
    #session_all.set_keyspace("road_geoloc")
    #rows = session_all.execute("SELECT * FROM header")
    array = []
    for row in rows:
        coords = row[1].split('\'')[0]
        if len(coords) is not 0:
            coord = []
            listofpairs = coords.split(';')
            for entry in listofpairs:
                lat, lon = entry.split(',')
                coord.append((float(lat), float(lon)))
            array.append(coord)
    formated = MultiLineString(array)
    my_feature = Feature(geometry=formated)
    coll = FeatureCollection([my_feature])
    print "Show all roads"
    return jsonify(coll)


@app.route("/batch/<rcid>")
def hichart(rcid):

    #query = "SELECT yyyymm, carcount FROM keyspace_batch.mytable_RDD WHERE roadid = %s"
    #response = session_batch.execute(query, parameters=[int(stid)])

    # ---------------------------------------------------------------------------------------------
    db = mdb.connect("localhost", "root", "", "traffic")    # Open database connection
    cursor = db.cursor()    # prepare a cursor object using cursor() method
    cursor.execute("SELECT yyyymm, count FROM batch WHERE rcid = %s" % str(rcid))
    response = cursor.fetchall()
    # ---------------------------------------------------------------------------------------------

    #query = "SELECT rdn FROM station_header.header WHERE stid = %s"
    #name = session_findname.execute(query, parameters=[stid])
    #stname = str(name[0].rdn.split('\'')[0])

    # ---------------------------------------------------------------------------------------------
    tup = header[str(rcid)]
    stname = tup[1] + ', ' + tup[2] + ', ' + tup[3]
    # ---------------------------------------------------------------------------------------------

    array = []
    yyyy = []
    mm = []
    for val in response:
        array.append(int(val[1]))
        key = str(val[0])
        yyyy.append(int(key[:-2]))
        mm.append(int(key[4:]))

    return render_template("batch_query.html", array=array, stname=stname, yyyy=yyyy, mm=mm)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
