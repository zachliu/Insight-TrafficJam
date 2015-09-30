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


@app.route("/real/")
def cassandra_test1():
    session_real.set_keyspace("keyspace_realtime")
    rows = session_real.execute("SELECT * FROM mytable")
    findname.set_keyspace("station_header")
    findcoor.set_keyspace("station_geoloc")

    def inner():
        yield """StID, &nbsp;&nbsp;&nbsp;&nbsp;Timestamp,
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            &nbsp;&nbsp;&nbsp;CC, &nbsp;&nbsp;&nbsp;Coord,
            &nbsp;&nbsp;&nbsp;Street Names<br/>\n"""
        print len(rows)
        cnt = 0
        for row in rows:
            stid = row[0].split('\'')[0]
            entry = findname.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
            coord = findcoor.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
            #print entry[0].stid
            if len(coord) is not 0:
                outputstr = row[0].split('\'')[0] + ', ' + \
                        row[1].split('\'')[0] + ', ' + \
                        row[2].split('\'')[0] + ', ' + \
                        coord[0].lat + ', ' + \
                        coord[0].lon + ', ' + \
                        entry[0].beg + ', ' + \
                        entry[0].end + ', ' + \
                        entry[0].rdn
                yield '%s<br/>\n' % outputstr
                cnt += 1
        print cnt
    return flask.Response(inner(), mimetype='text/html')


@app.route('/realtime')
def realtime():
    session_real.set_keyspace("keyspace_realtime")
    rows = session_real.execute("SELECT * FROM mytable")
    findname.set_keyspace("station_header")
    findcoor.set_keyspace("station_geoloc")
    cabs = []

    for row in rows:
        stid = row[0].split('\'')[0]
        coord = findcoor.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
        if len(coord) is not 0:
            cabs.append({'name': stid, 'lat': coord[0].lat, 'lng': coord[0].lon})
    return jsonify(cabs=cabs)


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


@app.route("/batch/")
def cassandra_test2():
    session_batch.set_keyspace("keyspace_batch")
    rows = session_batch.execute("SELECT * FROM mytable")
    findname.set_keyspace("station_header")
    findcoor.set_keyspace("station_geoloc")

    if rows is None:
        print "none"

    def inner():
        yield 'StID, &nbsp;&nbsp;&nbsp;&nbsp;Car Count<br/>\n'
        for row in rows:
            stid = row[0].split('\'')[0]
            entry = findname.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
            coord = findcoor.execute("SELECT * FROM header WHERE stid = '%s'" % stid)

            outputstr = row[0].split('\'')[0] + ', ' + \
                    row[1].split('\'')[0] + ', ' + \
                    row[2].split('\'')[0]
            yield '%s<br/>\n' % outputstr
            #cnt += 1
    #return jsonify(streets=streets)
    return flask.Response(inner(), mimetype='text/html')
    #return render_template('topicBycity.html',data =row[0])


@app.route("/batch_test/")
def cassandra_test3():
    session_rdd.set_keyspace("keyspace_batch")
    rows = session_rdd.execute("SELECT * FROM mytable_rdd")
    findname.set_keyspace("station_header")
    findcoor.set_keyspace("station_geoloc")
    if rows is None:
        print "none"

    def inner():
        yield 'StID, &nbsp;&nbsp;&nbsp;&nbsp;Car Count, &nbsp;&nbsp;&nbsp;&nbsp;Road Name<br/>\n'
        for row in rows:
            stid = row[0].split('\'')[0]
            entry = findname.execute("SELECT * FROM header WHERE stid = '%s'" % stid)
            if len(entry) is not 0:
                outputstr = row[0].split('\'')[0] + ', ' + \
                        row[1].split('\'')[0] + ', ' + \
                        entry[0].beg + ', ' + \
                        entry[0].end + ', ' + \
                        entry[0].rdn
            yield '%s<br/>\n' % outputstr
    return flask.Response(inner(), mimetype='text/html')


# ###################################################################################
# # top ten zip codes - daily
# @app.route('/top_ten_zipcode_daily')
# def top_ten_batch_zipcode_daily_search():
#     time_now_minus_two = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
#     return render_template("popular_zipcode_daily_search.html", time_now_minus_two=time_now_minus_two)
#
# @app.route("/top_ten_zipcode_daily", methods=['POST'])
# def top_ten_batch_zipcode_daily_post():
#     search_date = request.form["search_date"]
#     time_range = '[' + search_date + ']'
#
#     m = re.search('(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):\d\d:\d\d', search_date)
#     search_date = m.group(1) + "" + m.group(2) + "" + m.group(3)
#
#     stmt = "SELECT house_zipcode, count FROM trending_zipcode_by_day WHERE date = %s ALLOW FILTERING"
#     response = session.execute(stmt, parameters=[int(search_date)])
#
#     tmp_dict = {}
#     for val in response:
#         tmp_dict[val.house_zipcode] = val.count
#
#     tmp_dict_descending = OrderedDict(sorted(tmp_dict.items(), key=lambda kv: kv[1], reverse=True))
#
#     counter = 0
#     jsonresponse = []
#     for key in tmp_dict_descending:
#         if counter <= 10:
#             jsonresponse.append({"house_zipcode": key, "count": tmp_dict_descending[key]})
#         else:
#             break
#         counter += 1
#
#     jsonresponse = list(reversed(jsonresponse))
#     return render_template("popular_zipcode_daily.html", time_range=time_range, jsonresponse=jsonresponse)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
