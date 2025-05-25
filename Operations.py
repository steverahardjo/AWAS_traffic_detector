import os
import pandas as pd
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 pyspark-shell'
from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, element_at, when, broadcast

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
        self.batch_interval = batch_interval
        self.kafka_output_topic = kafka_output_topic
        self.eventSchema= StructType() \
                        .add("batch_id", IntegerType()) \
                        .add("event_id", StringType()) \
                        .add("car_plate", StringType()) \
                        .add("camera_id", IntegerType()) \
                        .add("timestamp", TimestampType()) \
                        .add("speed_reading", DoubleType()) \
                        .add("producer_id", StringType()) \
                        .add("sent_at", TimestampType())
        self.spark = SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
        

    def get_session(self):
        return self.spark
    
    def attach_kafka_stream(self, topic_name:str, hostip:str, watermark_time:str,port:int):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{hostip}:{port}")
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



class DbWriter:
    """
    Writes speed violation data to MongoDB.  This version is refactored.
    """
    def __init__(self, spark: SparkSession, mongo_uri: str, mongo_db: str, mongo_collection: str):
        """
        Initialize the DbWriter.

        Args:
            spark (SparkSession): The SparkSession (unused in this version, but kept for consistency).
            mongo_uri (str): The MongoDB connection URI.
            mongo_db (str): The name of the MongoDB database.
            mongo_collection (str): The name of the MongoDB collection for violations.
        """
        self.spark = spark
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db
        self.violation_collection_name = mongo_collection
        self.mongo_client = None
        self.db = None
        self.violation_coll = None

    def open(self, partition_id: str, epoch_id: str) -> bool:
        """
        Open a connection to MongoDB. Called at the start of each partition.
        """
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db_name]
        self.violation_coll = self.db[self.violation_collection_name]
        return True
    
    def process(self, row):
        data = row.asDict()
        print(f"\ncurrent data: {data}")

    def close(self, err: str) -> None:
        """
        Close the connection to MongoDB. Called at the end of processing.
        """
        if self.mongo_client:
            self.mongo_client.close()

    def add_violation(self, data, speed_limit, speed_reading):
        """
        Adds a violation record to the MongoDB collection.
        """
        ts = data["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        date_bucket = datetime(ts.year, ts.month, ts.day)
        violation = {
            "type": "instantaneous",
            "camera_id_start": data["camera_id"],
            "camera_id_end": None,
            "timestamp_start": ts,
            "timestamp_end": None,
            "measured_speed": speed_reading,
            "speed_limit": speed_limit,
        }

        existing_violation = self.violation_coll.find_one(
            {"car_plate": data["car_plate"], "date": date_bucket}
        )

        if existing_violation:
            original = existing_violation["violations"]
            original.append(violation)
            self.violation_coll.update_one(
                {"car_plate": data["car_plate"], "date": date_bucket},
                {"$set": {"violations": original}},
            )
            print(f"[MongoDB] Appended violation for {data['car_plate']} on {date_bucket}")
        else:
            self.violation_coll.insert_one({
                "violation_id": str(uuid.uuid4()),
                "car_plate": data["car_plate"],
                "date": date_bucket,
                "violations": [violation],
            })
            print(f"[MongoDB] Inserted new violation record for {data['car_plate']} on {date_bucket}")


class ConsoleWriter:

    def open(self, partition_id: str, epoch_id: str) -> bool:
        """
        Called at the start of each partition.
        """
        print(f"[ConsoleWriter] Open partition {partition_id}, epoch {epoch_id}")
        return True
    
    def process(self, row):
        data = row.asDict()
        print(f"\ncurrent data: {data}")

    def close(self, err: str) -> None:
        """
        Called at the end of processing.
        """
        if err:
            print(f"[ConsoleWriter] Closing with error: {err}")
        else:
            print(f"[ConsoleWriter] Closing successfully")

    def add_violation(self, data, speed_limit, speed_reading):
        """
        Prints a violation record to the terminal.
        """
        ts = data.get("timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        camera = data.get("camera_id")
        plate = data.get("car_plate")
        print(
            "[ConsoleWriter] Violation detected - "
            f"plate={plate}, camera={camera}, time={ts}, "
            f"speed={speed_reading}km/h, limit={speed_limit}km/h"
        )




    