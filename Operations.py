import os
import pandas as pd
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 pyspark-shell'
from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession
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
                        .add("producer_id", StringType()) \
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
            .option("startingOffsets", "earliest")    # ← add this
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), self.eventSchema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", watermark_time)
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
        self.spark = spark # Keep spark, even if not used.
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db
        self.violation_collection_name = mongo_collection
        self.mongo_client = None  # Initialize in open()
        self.db = None          # Initialize in open()
        self.violation_coll = None  # Initialize in open()

    def open(self, partition_id:str, epoch_id:str)->bool:
        """
        Open a connection to MongoDB.  Called at the start of each partition.

        Args:
            partition_id: The ID of the partition.
            epoch_id: The ID of the epoch.

        Returns:
            bool: True if the connection is successfully opened.
        """
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db_name]
        self.violation_coll = self.db[self.violation_collection_name]
        return True

    def close(self, err:str)->None:
        """
        Close the connection to MongoDB.  Called at the end of processing.

        Args:
            err: Any error that occurred during processing.  If None, processing was successful.
        """
        if self.mongo_client:
            self.mongo_client.close()

    def add_violation(self, data, speed_limit, speed_reading):
        """
        Adds a violation record to the MongoDB collection.

        Args:
            data:  A dictionary containing the violation data.
            speed_limit: The speed limit for the violation.
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

        existing_violation = self.violation_coll.find_one( # Use self.violation_coll
            {"car_plate": data["car_plate"], "date": date_bucket}
        )

        if existing_violation:
            original_violations = existing_violation["violations"]
            original_violations.append(violation)
            self.violation_coll.update_one(  # Use self.violation_coll
                {"car_plate": data["car_plate"], "date": date_bucket},
                {"$set": {"violations": original_violations}},
            )
            print(f"Added violation to car_plate {data['car_plate']} at {date_bucket}")
        else:
            self.violation_coll.insert_one( # Use self.violation_coll
                {
                    "violation_id": str(uuid.uuid4()),
                    "car_plate": data["car_plate"],
                    "date": date_bucket,
                    "violations": [violation],
                }
            )
            
# for debugging, print on console
class ConsoleWriter:
    def open(self, partition_id, epoch_id):
#         self.mongo_client = MongoClient(
#             host=f'172.22.32.1',
#             port=27017
#         )
#         self.db = self.mongo_client['fit3182_db']
#         self.camera_coll = self.db.Camera
        print("started")
        return True
    
    def process(self, row):
        data = row.asDict()
        print(f"\ncurrent data: {data}")
#         currentCameraData = self.camera_coll.find_one(
#                     {"_id": data["camera_id"]}
#                 )
#         print(f'current camera: {currentCameraData["_id"]}')
#         if float(currentCameraData["speed_limit"]) < float(data["speed_reading"]):
#             print(f"instant speed violated: {currentCameraData['speed_limit']} vs {data['speed_reading']}")
# #             self.db["Violation"].insert_one(self.createJsonSchema(data)) 
#             print(f"Generated schema: {self.createJsonSchema(data, currentCameraData['speed_limit'])}")
        
    # called once all rows have been processed (possibly with error)
    def close(self, err):
#         self.mongo_client.close()
        print("done")
        
    def createJsonSchema(self, data, speed_limit):
        # parse timestamp into a datetime if needed
        ts = data["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)

        # compute the “date” bucket = midnight on that day
        date_bucket = datetime(ts.year, ts.month, ts.day)

        # build the single-infraction subdoc
        violation = {
            "type":             "instantaneous",
            "camera_id_start":  data["camera_id"],
            "camera_id_end":    None,
            "timestamp_start":  ts,
            "timestamp_end":    None,
            "measured_speed":   float(data["speed_reading"]),
            "speed_limit":      float(speed_limit)
        }

        return {
            "violation_id": str(uuid.uuid4()),  # or f"{data['car_plate']}_{date_bucket.date()}"
            "car_plate":    data["car_plate"],
            "date":         date_bucket,
            "violations":   [violation]
        }