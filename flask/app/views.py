from app import app
from cassandra.cluster import Cluster
import flask
from flask import jsonify, render_template
#from flask import Flask
#from flask import jsonify, render_template, request
import json

#app = flask.Flask(__name__)

#@app.route("/")
#@app.route("/real/")
#def hello():
#    return "StID, Car Count"

#cluster = Cluster(['54.175.15.242'])
cluster = Cluster(['54.174.177.48'])
session_real = cluster.connect()
session_batch = cluster.connect()
session_rdd = cluster.connect()
session = cluster.connect()
session_all = cluster.connect()
findname = cluster.connect()
findcoor = cluster.connect()
findroad = cluster.connect()


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


@app.route('/realtime_roads')
def realtime_roads():
    session_real.set_keyspace("keyspace_realtime")
    rows = session_real.execute("SELECT * FROM mytable")
    findroad.set_keyspace("road_geoloc")
    roads = []

    for row in rows:
        stid = row[0].split('\'')[0]
        cc = row[2].split('\'')[0]
        locstring = findroad.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
        if len(locstring) is not 0:
            roadloc = []
            listofpairs = locstring[0].coord.split(';')
            for entry in listofpairs:
                roadloc.append(entry.split(','))
            roads.append({'name': stid, 'carcount': cc, 'roadloc': roadloc})
    return jsonify(roads=roads)


#@app.route('/all_roads')
#def all_roads():
    #session_all.set_keyspace("road_geoloc")
    #rows = session_all.execute("SELECT * FROM header")
    #roads = []
    #for row in rows:
        #stid = row[0].split('\'')[0]
        #locstring = row[1].split('\'')[0]
        #if len(locstring) is not 0:
            #roadloc = []
            #listofpairs = locstring.split(';')
            #for entry in listofpairs:
                #roadloc.append(entry.split(','))
            #roads.append({'name': stid, 'roadloc': roadloc})
    #return jsonify(roads=roads)


@app.route("/batch/<stid>")
def hichart(stid):
    #print stid
    stmt = "SELECT yyyymm, carcount FROM keyspace_batch.mytable_RDD WHERE roadid = %s"
    response = session.execute(stmt, parameters=[int(stid)])

    tmp_dict = {}
    for val in response:
        print val
        tmp_dict[val.yyyymm] = val.carcount

    #tmp_dict_descending = OrderedDict(sorted(tmp_dict.items(), key=lambda kv: kv[1], reverse=True))

    jsonresponse = []
    for key in tmp_dict:
        jsonresponse.append({"yyyymm": key, "carcount": tmp_dict[key]})

    jsonresponse = list(reversed(jsonresponse))
    return render_template("batch_test.html", jsonresponse=jsonresponse)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
