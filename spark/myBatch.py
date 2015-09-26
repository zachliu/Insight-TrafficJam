from pyspark import SparkContext, SparkConf
import pyspark_cassandra
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# set up cassandra
cluster = Cluster(['54.175.15.242'])
session = cluster.connect()
KEYSPACE = "keyspace_batch"

rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
if KEYSPACE in [row[0] for row in rows]:
    #log.debug("There is an existing keyspace...")
    #log.debug("setting keyspace...")
    session.set_keyspace(KEYSPACE)
else:
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
        """ % KEYSPACE)

    #log.debug("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    #log.debug("creating table...")
    session.execute("""
        CREATE TABLE mytable (
        thekey text,
        col1 text,
        col2 text,
        PRIMARY KEY (thekey)
        )
        """)

    session.execute("""
        CREATE TABLE mytable_RDD (
        thekey text,
        col1 text,
        PRIMARY KEY (thekey)
        )
        """)

query = SimpleStatement("""
    INSERT INTO mytable (thekey, col1, col2)
    VALUES (%(key)s, %(a)s, %(b)s)
    """, consistency_level=ConsistencyLevel.ONE)


# parse each line using only the 1st (stid) and the last element (car count)
def myParser(line):
    #if not line:
    ts, data = line.split('@')
    rows = data.split('#')
    lol = []    # list of lists
    for row in rows:
        foi = row.split(",")
        lol.append([foi[0], foi[-1]])
    return lol


print "Reading data from the HDFS"
conf = SparkConf().setAppName("myBatch")
sc = SparkContext(conf=conf)
data = sc.textFile("hdfs://ec2-54-175-15-242.compute-1.amazonaws.com:9000/user/TestData/traffic_1/20150925151400_0.txt")

# map reduce jop
formatted_data = data.flatMap(lambda line: myParser(line)).reduceByKey(lambda a, b: (int(a) + int(b)))

res = formatted_data.collect()    # colloect the result
for val in res:
    print val

print "Writing collected data to Cassandra"
# write individual entries to cassandra
for val in res:
    i = 0
    session.execute(query, dict(key="key%d" % i, a=val[0], b=str(val[1])))
    i += 1

print "Writing RDD to Cassandra (pyspark_cassandra)"
# write rdd to cassandra
formatted_data.saveToCassandra(
    "keyspace_batch",
    "mytable_rdd",
)