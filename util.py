import time
import json
from kafka3 import KafkaProducer, KafkaConsumer
from datetime import datetime as dt
from pymongo import MongoClient
from pyspark.sql import Row
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

class kafkaProducer:
    def __init__(self, csv_path: str, kafka_server: str, producer_id: str, topic: str, batch_interval: float = 5):
        """
        Initialize Kafka Producer with all necessary parameters.
        
        Args:
            csv_path (str): Path to the CSV file
            kafka_server (str): Kafka server address or env var name
            producer_id (str): Unique identifier for this producer
            topic (str): Kafka topic to produce to
            batch_interval (float, optional): Time between batches in seconds. Defaults to 5
        """
        # Load configuration from environment variables with fallback to provided values
        self.kafka_server = os.getenv('KAFKA_SERVER', kafka_server)
        self.producer_id = os.getenv('PRODUCER_ID', producer_id)
        self.topic = os.getenv('KAFKA_TOPIC', topic)
        self.batch_interval = batch_interval
        
        # Load data
        try:
            self.df = pd.read_csv(
                csv_path,
                sep=',',
                parse_dates=['timestamp'],
                dtype={'batch_id': int}
            )
            print(f"[INFO] Successfully loaded data from {csv_path}")
        except Exception as e:
            print(f"[ERROR] Failed to load CSV file {csv_path}: {e}")
            raise
        
        # Initialize producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_server],
                api_version=(0, 10),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8')
            )
            print(f"[INFO] Successfully connected to Kafka at {self.kafka_server}")
        except Exception as e:
            print(f"[ERROR] Failed to connect to Kafka at {self.kafka_server}: {e}")
            raise

    def produce_batches(self) -> None:
        """
        Iterate through each batch_id in order, wrap each record
        as a dict + producer tag, and send to Kafka. Then sleep.
        """
        for batch_id in sorted(self.df['batch_id'].unique()):
            batch_df = self.df[self.df['batch_id'] == batch_id]
            print(f"[INFO] Publishing batch #{batch_id} ({len(batch_df)} records)...")
            
            for _, row in batch_df.iterrows():
                event = row.to_dict()
                # Ensure timestamp is serializable
                event['timestamp'] = event['timestamp'].isoformat()
                # Tag with producer identity
                event['producer_id'] = self.producer_id
                # Record when the batch is sent exactly
                event['sent_at'] = dt.now().isoformat()

                try:
                    self.producer.send(
                        self.topic, 
                        key=event['car_plate'],
                        value=event,
                        timestamp_ms=int(time.time()*1000)
                    )
                except Exception as e:
                    print(f"[WARN] Failed to send event {event['event_id']}: {e}")

            # Force all buffered messages out
            self.producer.flush()
            print(f"[INFO] Batch #{batch_id} sent. Sleeping {self.batch_interval}s...")
            print(f"[DATA] Batch #{batch_id}:\n{batch_df}\n")
            time.sleep(self.batch_interval)
            
        # Loop has finished
        self.producer.flush()
        self.producer.close()
        print("[INFO] Producer finished and closed")

class kafkaConsumer:
    """
    Kafka Consumer class for consuming messages from Kafka topics.
    
    Args:
        kafka_topic (str): The Kafka topic to consume from
        kafka_server (str): The Kafka bootstrap server address
        consumer_id (str): Unique identifier for this consumer
        offset_time (str, optional): Offset reset strategy. Defaults to "earliest"
    """
    def __init__(self, kafka_topic: str, hostip:str, offset_time: str = "earliest"):
        self.kafka_topic = kafka_topic
        self.offset_time = offset_time
        self.consumer = None
        self.hostip = hostip
        self._connect()
    
    def _connect(self) -> None:
        """
        Internal method to establish connection to Kafka.
        Automatically called during initialization.
        """
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=[self.hostip],
                api_version=(0, 10),
                value_deserializer=lambda x: loads(x.decode('ascii')),
                auto_offset_reset=self.offset_time,
                group_id=self.consumer_id,
                enable_auto_commit=True
            )
            print(f"[INFO] Successfully connected to Kafka topic: {self.kafka_topic}")
        except Exception as e:
            print(f"[ERROR] Failed to connect to Kafka at {self.kafka_server}: {e}")
            raise

    def consume_messages(self):
        """
        Consume messages from the Kafka topic.
        Continuously polls for new messages and processes them.
        """
        if not self.consumer:
            raise ConnectionError("Consumer not initialized. Please check your connection settings.")
            
        try:
            print(f"[INFO] Starting to consume messages from topic: {self.kafka_topic}")
            for message in self.consumer:
                try:
                    message_value = message.value
                    print(f"[INFO] Received message: {message_value}")
                    # Add your message processing logic here
                    
                except Exception as msg_error:
                    print(f"[WARN] Error processing message: {msg_error}")
                    continue
                    
        except KeyboardInterrupt:
            print("[INFO] Consumer stopped by user")
        except Exception as e:
            print(f"[ERROR] Consumer error: {e}")
        finally:
            self.close()
    
    def close(self):
        """
        Safely close the Kafka consumer connection.
        """
        if self.consumer:
            try:
                self.consumer.close()
                print("[INFO] Kafka consumer connection closed")
            except Exception as e:
                print(f"[WARN] Error closing consumer: {e}")
    
    
    
    
