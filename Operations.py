import os
import pandas as pd
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 pyspark-shell'
from pymongo import MongoClient
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql.types import (
    StructType, StringType, IntegerType, DoubleType, TimestampType, BooleanType
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
        
        # immediately bump the KafkaDataConsumer logger to ERROR
        sc = self.spark.sparkContext
        jvm = sc._jvm
        LogManager = jvm.org.apache.log4j.LogManager
        Level      = jvm.org.apache.log4j.Level
        kafka_logger = LogManager.getLogger("org.apache.spark.sql.kafka010.KafkaDataConsumer")
        kafka_logger.setLevel(Level.ERROR)
        

    def get_session(self):
        return self.spark
    
    def attach_kafka_stream(self, topic_name:str, hostip:str, watermark_time:str):
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



class DbWriter():
    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_coll):
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db   = mongo_db
        self.mongo_coll = mongo_coll
        self.client     = None
        self.violation_coll  = None

    def open(self, partition_id: str, epoch_id: str) -> bool:
        from pymongo import MongoClient
        self.client = MongoClient(host=self.mongo_host, port=self.mongo_port)
        self.violation_coll  = self.client[self.mongo_db][self.mongo_coll]

        # Ensure index exists on car_plate and date
        self.violation_coll.create_index([("car_plate", 1), ("date", -1)], name="idx_compound_plate_date")
        self.violation_coll.create_index([("violation_id", 1)],unique=True, name="idx_violation_id")
        self.violation_coll.create_index([("date", 1)],name="idx_date")
        self.violation_coll.create_index([("violations.camera_id_start", 1)],name="idx_camera_start")
        self.violation_coll.create_index([("violations.camera_id_end", 1)],name="idx_camera_end")
        self.violation_coll.create_index([("date", 1), ("violations.measured_speed", -1)], name="idx_measured_speed")
        self.violation_coll.create_index([("violations.timestamp_start", 1)])
        return True

    def process(self, row):
        try:
            print(f"\nProcessing: {row.asDict()}")
            t_a = row.timestamp_a
            t_b = row.timestamp_b
            t_c = row.timestamp_c

            if isinstance(t_a, str):
                t_a = datetime.fromisoformat(t_a)

            if isinstance(t_b, str):
                t_b = datetime.fromisoformat(t_b)

            if isinstance(t_c, str):
                t_c = datetime.fromisoformat(t_c)

            date_bucket_a = datetime(t_a.year, t_a.month, t_a.day)
            date_bucket_b = datetime(t_b.year, t_b.month, t_b.day)
            date_bucket_c = datetime(t_c.year, t_c.month, t_c.day)

            violations_a = []
            violations_b = []
            violations_c = []

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
            if row.speed_flag_average_ab:
                violations_b.append({
                    "type": "average",
                    "camera_id_start": row.camera_id_a,
                    "camera_id_end": row.camera_id_b,
                    "timestamp_start": t_a,
                    "timestamp_end": t_b,
                    "measured_speed": row.avg_speed_reading_ab
                })
            if row.speed_flag_average_bc:
                violations_c.append({
                    "type": "average",
                    "camera_id_start": row.camera_id_b,
                    "camera_id_end": row.camera_id_c,
                    "timestamp_start": t_b,
                    "timestamp_end": t_c,
                    "measured_speed": row.avg_speed_reading_bc
                })

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
                        "violation_id": str(uuid.uuid4()),  # or f"{data['car_plate']}_{date_bucket
                        "car_plate":    row.car_plate,
                        "date":         date_bucket_a,
                        "violations":   violations_a
                    }
                )

            existing_b = self.violation_coll.find_one({"car_plate": row.car_plate, "date": date_bucket_b})
            if existing_b and len(violations_b) > 0:
                for violation in violations_b:
                    existing_b["violations"].append(violation)
                    self.violation_coll.update_one(
                        {"car_plate": row.car_plate, "date": date_bucket_b},
                        {"$set": {"violations": existing_b["violations"]}},
                    )
            elif len(violations_b) > 0:
                self.violation_coll.insert_one(
                    {
                        "car_plate":    row.car_plate,
                        "date":         date_bucket_b,
                        "violations":   violations_b
                    }
                )

            existing_c = self.violation_coll.find_one({"car_plate": row.car_plate, "date": date_bucket_c})                                    
            if existing_c and len(violations_c) > 0:
                for violation in violations_c:
                    existing_c["violations"].append(violation)
                    self.violation_coll.update_one(
                        {"car_plate": row.car_plate, "date": date_bucket_c},
                        {"$set": {"violations": existing_c["violations"]}},
                    )
            elif len(violations_c) > 0:
                self.violation_coll.insert_one(
                    {
                        "violation_id": str(uuid.uuid4()),  # or f"{data['car_plate']}_{date_bucket.date()}"
                        "car_plate":    row.car_plate,
                        "date":         date_bucket_c,
                        "violations":   violations_c
                    }
                )
#             print(f"\nAdded violations: {sum([len(violations_a),len(violations_b),len(violations_c)])}")
            if sum([len(violations_a),len(violations_b),len(violations_c)]) == 0 :
                   print("No violations detected for {row.car_plate} from {t_a} to {t_c}")
        except Exception as e:
            # this will print on the executor logs
            print(f"[DbWriter][ERROR] failed to process row {row}: {e}")
            # optionally, you could write to a dead‐letter collection instead
                                                  
    def close(self, error):
        if error:
            # this also shows up in the executor log
            print(f"[DbWriter][ERROR] task shutting down due to: {error}")
        if self.client:
            self.client.close()
            
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