import json

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.streaming.kafka import KafkaUtils

colproduct = "colproduct"
mongouri = "mongodb://localhost:27017/finaldb." + colproduct

conf = SparkConf().setMaster("local[2]") \
    .setAppName("Bicycle-Streaming-Consumer") \
    .set("spark.mongodb.input.uri", mongouri) \
    .set("spark.mongodb.output.uri", mongouri)

sc = SparkContext(conf=conf)

my_spark = SparkSession.builder \
    .appName("Bicycle-Streaming-Consumer") \
    .config("spark.mongodb.input.uri", mongouri) \
    .config("spark.mongodb.output.uri", mongouri) \
    .getOrCreate()

# Get the Kafka broker node
brokers = "localhost:2181"


# Get the exists topic named welcome-message
topic = "mytesttopic"

batchduration = 30  # in second

streamCtx = StreamingContext(sparkContext=sc, batchDuration=batchduration)
sparkCtx = streamCtx.sparkContext
sqlCtx = SQLContext(sparkContext=sparkCtx, sparkSession=my_spark)

kafkaParams = {"metadata.broker.list": [brokers],
                       "auto.offset.reset": "smallest"}
msg = KafkaUtils.createDirectStream(ssc=streamCtx, topics=[topic],kafkaParams=kafkaParams)

value = msg.map()
print(value)




streamCtx.start()
streamCtx.awaitTermination()
