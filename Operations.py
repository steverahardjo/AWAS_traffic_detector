import os
import pandas as pd
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 pyspark-shell'
from pymongo import MongoClient
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.types import (
    StructType, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.functions import (
    col, from_json
)
import uuid

class SparkInst:
    def __init__(self, app_name: str, batch_interval: int, kafka_output_topic: str):
        """
        Initializes a Spark instance with the given application name, batch interval, and Kafka topic.
        In here we enable:
            - spark job attachment with a kafka stream
            - kafka event schema being processed based on all 3 streams
            - instantiate a session with [ERROR] level ogging
            

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
        
        # immediately bump the KafkaDataConsumer logger to ERROR
        sc = self.spark.sparkContext
        jvm = sc._jvm
        LogManager = jvm.org.apache.log4j.LogManager
        Level      = jvm.org.apache.log4j.Level
        kafka_logger = LogManager.getLogger("org.apache.spark.sql.kafka010.KafkaDataConsumer")
        kafka_logger.setLevel(Level.ERROR)
        

    def get_session(self):
        """
        helper function to return a spark session instances
        """
        return self.spark
    
    def attach_kafka_stream(self, topic_name:str, hostip:str, watermark_time:str):
        """
        This method sets up a connection to a Kafka topic and processes incoming streaming 
        data using Spark. It expects data in JSON format and parses it according to the 
        predefined schema (`self.eventSchema`). A watermark is applied to enable proper 
        event-time aggregation and handling of late data.
        
        readStream parameters: 
        - topic_name: the name of kafka stream, spark session subscribe to
        - hostip: ip address where we can communicate with kafka
        - watermark: time threshold to allow data coming out late outside of window being specifie in each queries.
        """
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{hostip}:9092")
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), self.eventSchema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", watermark_time)
        )
    

    def essentialData_broadcast(self, sdf):
        """
        Filter a Spark DataFrame by topic_id and broadcast it as a part of UDF later on
        accross different subjobs.

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
    def __init__(self, mongo_host:str, mongo_port:int, mongo_db:str, mongo_coll:str):
        """
        This is the DbWriter implemented to be in line with the pySpark's forEach() inside sink writer.
        This class is created so that pySpark session enabled to write data by initialized a 
        pyMongo client access to a DB and a Collection.
        Input:
            mongo_host: ip address where mongoDB is located.
            mongo port: port to access mongoDB
            mongo_db: the name of mongo database inside the client
            mongo_coll: mongo collection we want to insert into
        """
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db   = mongo_db
        self.mongo_coll = mongo_coll
        self.client     = None
        self.violation_coll  = None

    def open(self, partition_id: str, epoch_id: str) -> bool:
        """
        Establlish a connection with a MongoDB client. This function going to auto-triggered park 
        Structured Streaming at the start of processing a new partition for a given epoch.
        We reuse the parameters being uploaded in function __init__()
        """
        self.client = MongoClient(host=self.mongo_host, port=self.mongo_port)
        self.violation_coll  = self.client[self.mongo_db][self.mongo_coll]
        return True

    def process(self, row)-> None:
        """
        Processes a single row of streaming data to detect and log traffic speed violations.

        This method is invoked for each row in a Spark Structured Streaming batch. It checks
        for both instantaneous and average speed violations captured by three cameras (A, B, C),
        and stores the violations in a MongoDB collection grouped by car plate and date.

        The steps include:
        - Parsing timestamps to ensure correct datetime format.
        - Detecting instantaneous violations at each camera.
        - Detecting average speed violations between camera pairs (A-B and B-C).
        - Organizing violations by the date bucket (per day).
        - Checking if a document for the car and date already exists:
            - If yes, append new violations.
            - If no, insert a new document.
            
        Input:
            row : pyspark.sql.Row
        """
        try:
            print(f"\nProcessing: {row.asDict()}")
            t_a = row.timestamp_a
            t_b = row.timestamp_b
            t_c = row.timestamp_c
            
            #process in to a isofromat datetime
            if isinstance(t_a, str):
                t_a = datetime.fromisoformat(t_a)

            if isinstance(t_b, str):
                t_b = datetime.fromisoformat(t_b)

            if isinstance(t_c, str):
                t_c = datetime.fromisoformat(t_c)
                
            #convert isoformat datetime to a date
            date_bucket_a = datetime(t_a.year, t_a.month, t_a.day)
            date_bucket_b = datetime(t_b.year, t_b.month, t_b.day)
            date_bucket_c = datetime(t_c.year, t_c.month, t_c.day)

            violations_a = []
            violations_b = []
            violations_c = []
            
            #if condition for instant violation in camera A
            if row.speed_flag_instant_a:
                violations_a.append({
                    "violation_id": str(uuid.uuid4()),
                    "type": "instantaneous",
                    "camera_id_start": row.camera_id_a,
                    "camera_id_end": None,
                    "timestamp_start": t_a,
                    "timestamp_end": None,
                    "measured_speed": row.speed_reading_a
                })
                
            #if condition for instant violation in camera B
            if row.speed_flag_instant_b:
                violations_b.append({
                    "violation_id": str(uuid.uuid4()),
                    "type": "instantaneous",
                    "camera_id_start": row.camera_id_b,
                    "camera_id_end": None,
                    "timestamp_start": t_b,
                    "timestamp_end": None,
                    "measured_speed": row.speed_reading_b
                })
                
            #if condition for instant violation in camera C
            if row.speed_flag_instant_c:
                violations_c.append({
                    "violation_id": str(uuid.uuid4()),
                    "type": "instantaneous",
                    "camera_id_start": row.camera_id_c,
                    "camera_id_end": None,
                    "timestamp_start": t_c,
                    "timestamp_end": None,
                    "measured_speed": row.speed_reading_c
                })
                
            #if condition for average violation between camera A and B
            if row.speed_flag_average_ab:
                violations_b.append({
                    "violation_id": str(uuid.uuid4()),
                    "type": "average",
                    "camera_id_start": row.camera_id_a,
                    "camera_id_end": row.camera_id_b,
                    "timestamp_start": t_a,
                    "timestamp_end": t_b,
                    "measured_speed": row.avg_speed_reading_ab
                })
                
            #if condition for average violation between camera A and B
            if row.speed_flag_average_bc:
                violations_c.append({
                    "violation_id": str(uuid.uuid4()),
                    "type": "average",
                    "camera_id_start": row.camera_id_b,
                    "camera_id_end": row.camera_id_c,
                    "timestamp_start": t_b,
                    "timestamp_end": t_c,
                    "measured_speed": row.avg_speed_reading_bc
                })
            #update violation in time A, if search through collection with car_plate and date it exist.
            existing_a = self.violation_coll.find_one({"car_plate": row.car_plate, "date": date_bucket_a})
            if existing_a and len(violations_a) > 0:
                for violation in violations_a:
                    existing_a["violations"].append(violation)
                    self.violation_coll.update_one(
                        {"car_plate": row.car_plate, "date": date_bucket_a},
                        {"$set": {"violations": existing_a["violations"]}},
                    )
            
            elif len(violations_a) > 0:
                self.violation_coll.insert_one(
                    {
                        "car_plate":    row.car_plate,
                        "date":         date_bucket_a,
                        "violations":   violations_a
                    }
                )
            #update violation in time B, if search through collection with car_plate and date it exist.
            existing_b = self.violation_coll.find_one({"car_plate": row.car_plate, "date": date_bucket_b})
            if existing_b and len(violations_b) > 0:
                for violation in violations_b:
                    existing_b["violations"].append(violation)
                    self.violation_coll.update_one(
                        {"car_plate": row.car_plate, "date": date_bucket_b},
                        {"$set": {"violations": existing_b["violations"]}},
                    )
            # if not, create a new collection with all the violations we can get inside
            elif len(violations_b) > 0:
                self.violation_coll.insert_one(
                    {
                        "car_plate":    row.car_plate,
                        "date":         date_bucket_b,
                        "violations":   violations_b
                    }
                )
            #update violation in time C, if search through collection with car_plate and date it exist.
            existing_c = self.violation_coll.find_one({"car_plate": row.car_plate, "date": date_bucket_c})                                    
            if existing_c and len(violations_c) > 0:
                for violation in violations_c:
                    existing_c["violations"].append(violation)
                    self.violation_coll.update_one(
                        {"car_plate": row.car_plate, "date": date_bucket_c},
                        {"$set": {"violations": existing_c["violations"]}},
                    )
            # if not, create a new collection with all the violations we can get inside
            elif len(violations_c) > 0:
                self.violation_coll.insert_one(
                    {
                        "car_plate":    row.car_plate,
                        "date":         date_bucket_c,
                        "violations":   violations_c
                    }
                )
            if sum([len(violations_a),len(violations_b),len(violations_c)]) == 0 :
                   print("No violations detected for {row.car_plate} from {t_a} to {t_c}")
        except Exception as e:
            # this will print on the executor logs
            print(f"[DbWriter][ERROR] failed to process row {row}: {e}")
                                                  
    def close(self, error):
        if error:
            # this also shows up in the executor log
            print(f"[DbWriter][ERROR] task shutting down due to: {error}")
        if self.client:
            self.client.close()
        