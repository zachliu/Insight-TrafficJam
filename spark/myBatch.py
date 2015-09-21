from pyspark import SparkContext, SparkConf
import pyspark_cassandra

def myParser(line):
    b = line.split(",")
    return [b[0], b[-1]]


conf = SparkConf().setAppName("myBatch")
sc = SparkContext(conf=conf)
data = sc.textFile("hdfs://ec2-54-175-15-242.compute-1.amazonaws.com:9000/user/TestData/my-topic/20150920193500_0.txt")
formatted_data = data.map(lambda line: myParser(line)).reduceByKey(lambda a,b: (int(a)+int(b)))
res = formatted_data.collect()

for val in res:
    print val

print "Reading data from the HDFS"

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

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
        PRIMARY KEY (thekey, col1)
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

# write individual entries to cassandra
for val in res:
    i = 122330
    session.execute(query, dict(key="key%d" % i, a=val[0], b=str(val[1])))
    i += 1

print "Writing data to Cassandra"

print "Test for pyspark_cassandra"

# write rdd to cassandra
formatted_data.saveToCassandra(
    "keyspace_batch",
    "mytable_rdd",
)