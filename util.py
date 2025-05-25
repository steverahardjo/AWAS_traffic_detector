import time
import json
from kafka3 import KafkaProducer, KafkaConsumer
from datetime import datetime as dt
from pymongo import MongoClient
from pyspark.sql import Row
import pandas as pd
import datetime as dt
from json import loads
import matplotlib.pyplot as plt
from collections import Counter
from typing import List


class kafkaProducer:
    def __init__(self, csv_path: str, kafka_server: str, producer_id: int, topic: str, batch_interval: float = 5):
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
        self.kafka_server = kafka_server
        self.producer_id = producer_id
        self.topic = topic
        self.batch_interval = batch_interval
        
        # Load data
        try:
            self.df = pd.read_csv(
                csv_path,
                sep=',',
                parse_dates=['timestamp'],
                dtype={'batch_id': int}
            )
            self.df.rename(columns=lambda x: x.strip(), inplace=True)
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
            print(f"print {self.kafka_server}")

        except Exception as e:
            print(f"[ERROR] Failed to connect to Kafka at {self.kafka_server}: {e}")
            raise

    def produce_batches(self) -> None:
        """
        Iterate through each batch_id in order, wrap each record
        as a dict + producer tag, and send to Kafka. Then sleep.
        """
        print(self.kafka_server)
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
                event['sent_at'] = dt.datetime.now().isoformat()

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
                bootstrap_servers=[f'{self.hostip}:9092'],
                api_version=(0, 10),
                value_deserializer=lambda x: loads(x.decode('ascii')),
                auto_offset_reset=self.offset_time,
                enable_auto_commit=True,
                consumer_timeout_ms=1000
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



class Plotter:
    def __init__(self, width=9.5, height=6, title='Real-time stream data visualization'):
        self.width = width
        self.height = height
        self.title = title

        try:
            # Create figure and axis
            self.fig, self.ax = plt.subplots(figsize=(self.width, self.height))
            self.ax.set_xlabel('Time')
            self.ax.set_ylabel('Value')
            self.ax.set_title(self.title)

            # Display the figure
            self.fig.show()
            self.fig.canvas.draw()

        except Exception as ex:
            print(f"Error initializing plot: {str(ex)}")

    def get_axis(self):
        return self.ax

    def get_figure(self):
        return self.fig
    
    def add_min_max(self, x_data:List[int], y_data:List[int]):
        min_y = min(y_data)
        max_y = max(y_data)
        ypos_min=y_data.index(min_y)
        ypos_max=y_data.index(max_y)
        self.ax.annotate(f'Min: {min_y}', xy=(0, min_y), xytext=(0, min_y-10))      
        self.ax.annotate(f'Max: {max_y}', xy=(0, max_y), xytext=(0, max_y+10))

    def stream_asPlot(self, consumer: KafkaConsumer):
        
        hour_counts = Counter()

        if consumer:
            for message in consumer.consume_messages():
                # Parse and truncate timestamp to the hour
                dt_obj = datetime.fromisoformat(message["timestamp_start"])
                dt_hour = dt_obj.replace(minute=0, second=0, microsecond=0)
                
                # Update count for the hour
                hour_counts[dt_hour] += 1

                # Optional: Real-time plot update (simplified version)
                hours_sorted = sorted(hour_counts)
                counts_sorted = [hour_counts[hr] for hr in hours_sorted]

                self.ax1.clear()
                self.ax1.plot(hours_sorted, counts_sorted, marker='o', label="Violations")
                self.ax1.set_title("Violations per Hour")
                self.ax1.set_xlabel("Hour")
                self.ax1.set_ylabel("Count")
                self.ax1.tick_params(axis='x', rotation=45)
                self.ax1.legend()
                self.add_min_max(hours_sorted, counts_sorted)
                self.fig.canvas.draw()
                self.fig.canvas.flush_events()

                if len(hours_sorted) > 10 and len(counts_sorted) > 10:
                    plt.pause(0.1)
                    hours_sorted.pop(0)
                    counts_sorted.pop(0)
                

    
                    


