import os
import pandas as pd
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 pyspark-shell'
from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession, ForeachWriter
from pyspark.sql.functions import col

from pyspark.sql.types import (
    StructType, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.functions import (
    col, expr, from_json
)
import uuid

class SparkInst:
    def __init__(self, app_name: str, batch_interval: int, kafka_output_topic: str):
        """
        Initializes a Spark instance with the given application name, batch interval, and Kafka topic.

        Args:
            app_name (str): The name of the Spark application.
            batch_interval (int): The interval (in seconds) at which streaming data is processed.
            kafka_topic (str): The name of the Kafka topic to consume from.
        """
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 pyspark-shell'
        self.batch_interval = batch_interval
        self.kafka_output_topic = kafka_output_topic
        self.eventSchema= StructType() \
                        .add("batch_id", IntegerType()) \
                        .add("event_id", StringType()) \
                        .add("car_plate", StringType()) \
                        .add("camera_id", IntegerType()) \
                        .add("timestamp", TimestampType()) \
                        .add("speed_reading", DoubleType()) \
                        .add("producer", StringType()) \
                        .add("sent_at", TimestampType())
        self.spark = SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
        

    def get_session(self):
        return self.spark
    
    def attach_kafka_stream(self, topic_name:str, hostip:str, watermark_time:str):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{hostip}:9092")
            .option("subscribe", topic_name)
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), self.eventSchema).alias("data"))
            .select("data.*")
            .withWatermark("sent_at", watermark_time)
        )
    

    def essentialData_broadcast(self, sdf):
        """
        Filter a Spark DataFrame by topic_id and broadcast it.

        Args:
            sdf (DataFrame): Spark DataFrame

        Returns:
            Broadcast variable containing a dictionary of camera_id to speed_limit
        """
        # Select necessary columns
        df_filtered = sdf.select("camera_id", "speed_limit")

        # Convert to a Python dictionary (camera_id -> speed_limit)
        data = df_filtered.rdd.map(lambda row: (row["camera_id"], row["speed_limit"])).collectAsMap()

        # Broadcast the dictionary
        spark_context = self.spark.sparkContext
        return spark_context.broadcast(data)



class DbWriter(ForeachWriter):
    def __init__(self, mongo_uri: str, mongo_db: str, mongo_collection: str, speed_limit: float):
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db
        self.violation_collection_name = mongo_collection
        self.mongo_client = None
        self.db = None
        self.violation_coll = None
        self.speed_limit = speed_limit

    def open(self, partition_id: str, epoch_id: str) -> bool:
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db_name]
        self.violation_coll = self.db[self.violation_collection_name]
        return True

    def process(self, row):
        t_start = row.timestamp_start
        t_end = row.timestamp_end

        if isinstance(t_start, str):
            t_start = datetime.fromisoformat(t_start)

        if isinstance(t_end, str):
            t_end = datetime.fromisoformat(t_end)

        date_bucket = datetime(t_start.year, t_start.month, t_start.day)

        if row.speed_flag_instant_start != None and row.timestamp_end == None:
            violation={
                "type": "instantaneous",
                "camera_id_start": row.camera_id_start,
                "camera_id_end": None,
                "timestamp_start": t_start,
                "timestamp_end": None,
                "measured_speed": row.speed_reading
            },
        elif row.speed_flag_instant_end != None and row.timestamp_start == None:
            violation={
                "type": "instantaneous",
                "camera_id_start": None,
                "camera_id_end": row.camera_id_end,
                "timestamp_start": None,
                "timestamp_end": t_end,
                "measured_speed": row.speed_reading
            }
        else:
            violation={
                "type":"average",
                "camera_id_start": row.camera_id_start,
                "camera_id_end": row.camera_id_end,
                "timestamp_start": t_start,
                "timestamp_end": t_end,
                "measured_speed": row.speed_reading
            }
        existing = self.violation_coll.find_one({"car_plate": row.car_plate, "date": date_bucket})
        if existing:
            existing["violations"].append(violation)
            self.violation_coll.update_one(
                {"car_plate": row.car_plate, "date": date_bucket},
                {"$set": {"violations": existing["violations"]}},
            )
        else:
            self.violation_coll.insert_one({
                "violation_id": str(uuid.uuid4()),
                "car_plate": row.car_plate,
                "date": date_bucket,
                "violations": [violation],
            })

    def close(self, error):
        if self.mongo_client:
            self.mongo_client.close()