from app import app
from cassandra.cluster import Cluster
import flask
from flask import jsonify, render_template
import json
import geojson
from geojson import Feature, MultiLineString, FeatureCollection
import time

#app = flask.Flask(__name__)

#cluster = Cluster(['54.175.15.242'])
cluster = Cluster(['54.174.177.48'])
session_real = cluster.connect()
session_batch = cluster.connect()
session_findname = cluster.connect()
session_findloc = cluster.connect()

session_findloc.set_keyspace("road_geoloc")
start = time.time()
lookup = {}
cass = session_findloc.execute("SELECT * FROM header")
#print cass

for e in cass:
    lookup[str(e.stid)] = str(e.coord)
print time.time() - start
#print lookup


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/maps')
def maps():
    return render_template('index.html')


@app.route('/aboutme')
def aboutme():
    return render_template('aboutme.html')


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

hashtable = {}

@app.route('/realtime_roads')
def realtime_roads():
    session_real.set_keyspace("keyspace_realtime")
    rows = session_real.execute("SELECT * FROM mytable")
    #session_findloc.set_keyspace("road_geoloc")
    roads = []
    total = 0

    #start = time.time()
    #lookup = {}
    #cass = session_findloc.execute("SELECT * FROM header")
    ##print cass

    #for e in cass:
        #lookup[str(e.stid)] = str(e.coord)
    #print time.time() - start
    ##print lookup

    for row in rows:
        stid = str(row[0].split('\'')[0])
        cc = str(row[2].split('\'')[0])

        if (stid not in hashtable) or ((stid in hashtable) and (hashtable[stid] is not chooseColor(cc))):
            hashtable[stid] = chooseColor(cc)
            start = time.time()
            locstring = ''
            if stid in lookup:
                locstring = lookup[stid]
            #locstring = session_findloc.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
            total += time.time() - start

            if len(locstring) is not 0:
                roadloc = []
                #listofpairs = locstring[0].coord.split(';')
                listofpairs = locstring.split(';')
                for entry in listofpairs:
                    roadloc.append(entry.split(','))
                roads.append({'name': stid, 'carcount': cc, 'roadloc': roadloc})
    print "refresh realtime roads info " + str(len(roads)) + " " + str(total)
    return jsonify(roads=roads)


session_all = cluster.connect()
@app.route('/all_roads')
def all_roads():
    session_all.set_keyspace("road_geoloc")
    rows = session_all.execute("SELECT * FROM header")
    #roads = []
    array = []
    for row in rows:
        #stid = row[0].split('\'')[0]
        locstring = row[1].split('\'')[0]
        if len(locstring) is not 0:
            roadloc = []
            listofpairs = locstring.split(';')
            for entry in listofpairs:
                lat, lon = entry.split(',')
                roadloc.append((float(lat), float(lon)))
            #roads.append({'name': stid, 'roadloc': roadloc})
            array.append(roadloc)
    #print roadloc
    formated = MultiLineString(array)
    my_feature = Feature(geometry=formated)
    coll = FeatureCollection([my_feature])
    print "hello i am called"
    return jsonify(coll)
    #return jsonify(formated=formated)


@app.route("/batch/<stid>")
def hichart(stid):
    #print stid
    query = "SELECT yyyymm, carcount FROM keyspace_batch.mytable_RDD WHERE roadid = %s"
    response = session_batch.execute(query, parameters=[int(stid)])

    query = "SELECT rdn FROM station_header.header WHERE stid = %s"
    name = session_findname.execute(query, parameters=[stid])
    stname = str(name[0].rdn.split('\'')[0])
    print type(stname)
    tmp_dict = {}
    for val in response:
        print val
        tmp_dict[val.yyyymm] = val.carcount

    #tmp_dict_descending = OrderedDict(sorted(tmp_dict.items(), key=lambda kv: kv[1], reverse=True))

    array = []
    for key in tmp_dict:
        array.append(tmp_dict[key])
        #jsonresponse.append({"yyyymm": key, "carcount": tmp_dict[key]})

    #jsonresponse = list(reversed(jsonresponse))
    return render_template("batch_query.html", array=array, stname=stname)
    #return jsonify(jsonresponse=jsonresponse)

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
