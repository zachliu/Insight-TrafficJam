#TrafficJam
<img src="https://github.com/zachliu/Insight-TrafficJam/blob/master/images/traffic.jpg" alt="alt text" width="640" height="400">


#Table of Contents
- <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/README.md#introduction">Introduction</a>
- <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/README.md#data-set">Data Set</a>
- <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/README.md#data-transformations">Data Transformations</a>
- <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/README.md#live-demo">Live Demo</a>
- <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/README.md#presentation-deck">Presentation Deck</a>
- <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/README.md#instructions-to-run-this-pipeline">Instructions to Run this Pipeline</a>


#Introduction
This is a data engineering project at Insight Data Science. There are two goals that this project aims to accomplish:
- Provide an API for drivers, city planners, and data scientists, for analyzing long term trends in traffic pattern w.r.t metrics such as average car volume, speed etc.
- Enable a framework for real-time monitoring of traffic information, so that a user can know the best route to take and select a specific road to view the historical data.

#Data Set
**Historical:**
The project is based on historical traffic volume data for nearly 60,000 major roads in New York State, collected over 10 years. The data is available as a time series. The following table provides a snap shot of the raw data set:

roadID, timestamp, car count

<img src="https://github.com/zachliu/Insight-TrafficJam/blob/master/images/rawdata.png" alt="alt text" width="408" height="216">

**Real-Time:**
The historical dataset is played back to simulate real-time behavior.

**AWS Clusters:**
A distributed AWS cluster of four ec2 machines is being used for this project. All the components (ingestion, batch and real-time processing) are configured and run in distributed mode, with one master and three workers.

#Data Processing Framework
<img src="https://github.com/zachliu/Insight-TrafficJam/blob/master/images/pipeline.png" alt="alt text" width="600" height="254">

- **Ingestion Layer (Kafka):** The raw data is consumed by a message broker, configured in publish-subscribe mode. Related files: <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/kafka/producer.py">producer.py</a>, <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/kafka/kafka_consumer.py">kafka_consumer.py</a>.

- **Batch Layer (HDFS, Spark):** A kafka consumer stores the data into HDFS. Additional columns are added to the dataset to generate metrics as described in the previous section. Following this, tables representing the aggregate views for serving queries at the user end are generated using Spark. Related Files: <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/spark/myBatch.py">myBatch.py</a>  

- **Speed Layer (Storm):** The topology for processing real-time data comprises of a kafka-spout and a bolt (with tick interval frequency of 2.5 sec). The data is filtered to only store clean (uncorrupted) entries. Related files: <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/Storm/cab_topology/cab_topology/stormBolt.py">stormBolt.py</a>, <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/Storm/cab_topology/topology.yaml">topology.yaml</a>

- **Front-end (Flask):** The car volume information for each road are rendered on Google Maps in terms of four colors and updated at 1 sec interval via Flask. Historical data is represented as line plot via Highcharts. Realted files: <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/flask/app/views.py">views.py</a>, <a href= "https://github.com/zachliu/Insight-TrafficJam/blob/master/flask/app/static/batch.js">batch.js</a>, <a href="https://github.com/zachliu/Insight-TrafficJam/blob/master/flask/app/static/map.js">map.js</a>.

- **Libraries and APIs:** Cassandra, Pyleus, Kafka-python, Google Maps

#Data Transformations
Following metrics are computed via a MapReduce operation on the raw dataset (Spark):
- Total car count in a month

#Live Demo:
A Live Demo of the project is available here: <a href= "http://trafficjam.today">trafficjam.today</a> or <a href= "http://trafficjam.online">trafficjam.online</a>. A snap shot of the map with highlighted roads:


<img src="https://github.com/zachliu/Insight-TrafficJam/blob/master/images/realtime.png" alt="alt text" width="707" height="542">

#Presentation
The presentation slides are available here:
<a href= "http://trafficjam.today/slideshare">trafficjam.today/slideshare</a>

#Instructions to Run this Pipeline

Install python packages:
```sudo pip install kafka-python cassandra-driver pyleus```

Run the Kafka producer:
```python kafka/producer.py```

Run the Kafka consumer:
```python kafka/kafka_consumer.py```

Run Spark:
```spark-submit --packages TargetHolding/pyspark-cassandra:0.1.5 ~/Insight-TrafficJam/spark/myBatch.py```


Build storm topology:
```pyleus build topology.yaml```

Submit pyleus topology:
```pyleus submit -n 54.174.177.48 topology.jar```






