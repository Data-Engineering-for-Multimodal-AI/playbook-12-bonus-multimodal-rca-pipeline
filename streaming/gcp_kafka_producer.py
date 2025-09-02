"""
GCP-Integrated Kafka Producer for Future Factories Data
Reads real data from GCS and streams to Kafka with enterprise patterns
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from confluent_kafka import Producer
from google.cloud import storage
import numpy as np
import pandas as pd
from pathlib import Path
import hashlib
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class FutureFactoriesEvent:
 """Event structure for Future Factories data"""
 timestamp: str
 factory_id: str = "future-factories-usc"
 session_id: str = "ff-streaming-session"
 event_type: str = "sensor_reading"
 sensor_data: Dict[str, Any] = None
 image_references: List[str] = None
 edge_processed: bool = True
 gcp_ingested_at: str = None
 compression_applied: bool = False

class GCPKafkaProducer:
 """
 Advanced Kafka producer that reads Future Factories data from GCS
 and streams it with enterprise data engineering patterns
 """
 
 def __init__(
 self,
 gcs_bucket: str = "resolve-agent-pipeline-resolve-binaries",
 kafka_bootstrap_servers: str = "localhost:9092",
 project_id: str = "resolve-agent-pipeline"
 ):
 self.gcs_bucket = gcs_bucket
 self.project_id = project_id
 self.storage_client = storage.Client(project=project_id)
 
 # Kafka producer configuration for high throughput
 producer_config = {
 'bootstrap.servers': kafka_bootstrap_servers,
 'client.id': 'future-factories-producer',
 'acks': 'all',
 'retries': 3,
 'batch.size': 32768, # Larger batches for better throughput
 'linger.ms': 10, # Slight delay for batching
 'compression.type': 'lz4',
 'enable.idempotence': True,
 'max.in.flight.requests.per.connection': 5,
 }
 
 self.producer = Producer(producer_config)
 
 # Metrics
 self.metrics = {
 'records_processed': 0,
 'events_published': 0,
 'bytes_sent': 0,
 'gcs_files_read': 0,
 'start_time': time.time()
 }
 
 logger.info(f"Initialized GCP Kafka producer for bucket: {gcs_bucket}")
 
 def read_future_factories_data(self, batch_files: List[str] = None) -> List[Dict[str, Any]]:
 """Read Future Factories data from GCS"""
 
 if not batch_files:
 batch_files = ["data_31000.json", "data_32000.json"]
 
 all_records = []
 
 try:
 bucket = self.storage_client.bucket(self.gcs_bucket)
 
 for file_name in batch_files:
 blob_path = f"datasets/future-factories/multimodal-2024-data/Batch 1/{file_name}"
 logger.info(f"Reading {blob_path} from GCS...")
 
 blob = bucket.blob(blob_path)
 if not blob.exists():
 logger.warning(f"File not found: {blob_path}")
 continue
 
 # Download and parse JSON
 json_data = blob.download_as_text()
 data = json.loads(json_data)
 
 # Convert to streaming records
 for timestamp, record in data.items():
 sensor_values = record.get("Sensor_values", {})
 images = record.get("Images", [])
 
 # Create streaming event
 event = {
 'original_timestamp': timestamp,
 'ingestion_timestamp': datetime.now().isoformat(),
 'sensor_data': sensor_values,
 'image_references': images,
 'source_file': file_name,
 'gcs_bucket': self.gcs_bucket,
 'batch_id': file_name.replace('.json', ''),
 'record_hash': hashlib.md5(json.dumps(sensor_values, sort_keys=True).encode()).hexdigest()[:16]
 }
 
 all_records.append(event)
 
 self.metrics['gcs_files_read'] += 1
 logger.info(f"Processed {file_name}: {len(data)} records")
 
 except Exception as e:
 logger.error(f"Error reading from GCS: {e}")
 return []
 
 logger.info(f"Total records loaded: {len(all_records)}")
 return all_records
 
 async def stream_future_factories_data(
 self, 
 records: List[Dict[str, Any]], 
 topic: str = "future-factories-telemetry",
 streaming_rate_hz: float = 10.0
 ):
 """Stream Future Factories data to Kafka at specified rate"""
 
 logger.info(f"Starting stream to topic '{topic}' at {streaming_rate_hz} Hz")
 
 delay_between_records = 1.0 / streaming_rate_hz
 
 for i, record in enumerate(records):
 try:
 # Create Future Factories event
 ff_event = FutureFactoriesEvent(
 timestamp=record['ingestion_timestamp'],
 event_type='multimodal_reading',
 sensor_data=record['sensor_data'],
 image_references=record['image_references'],
 gcp_ingested_at=datetime.now().isoformat()
 )
 
 # Serialize event
 event_json = json.dumps(asdict(ff_event), default=str)
 
 # Create partition key for ordered processing
 partition_key = f"{ff_event.factory_id}-{record['batch_id']}"
 
 # Send to Kafka
 self.producer.produce(
 topic=topic,
 key=partition_key,
 value=event_json,
 on_delivery=self._delivery_callback
 )
 
 # Update metrics
 self.metrics['records_processed'] += 1
 self.metrics['events_published'] += 1
 self.metrics['bytes_sent'] += len(event_json)
 
 # Log progress
 if i % 50 == 0:
 logger.info(f"Streamed {i+1}/{len(records)} records")
 
 # Non-blocking flush periodically
 if i % 100 == 0:
 self.producer.poll(0)
 
 # Rate limiting
 await asyncio.sleep(delay_between_records)
 
 except Exception as e:
 logger.error(f"Error streaming record {i}: {e}")
 
 # Final flush
 self.producer.flush(timeout=30)
 logger.info(f"Completed streaming {len(records)} records")
 
 async def stream_with_gcp_analytics_integration(self, records: List[Dict[str, Any]]):
 """Stream data with GCP BigQuery integration for analytics"""
 
 # Stream to multiple topics for different processing patterns
 topics = {
 'future-factories-raw': 'Raw sensor data for archival',
 'future-factories-analytics': 'Processed data for real-time analytics', 
 'future-factories-ml': 'Feature-engineered data for ML pipelines'
 }
 
 logger.info(f"Streaming to multiple GCP-integrated topics...")
 
 for topic, description in topics.items():
 logger.info(f"Streaming to {topic}: {description}")
 
 # Transform data based on topic purpose
 if topic == 'future-factories-analytics':
 analytics_records = self._prepare_analytics_data(records)
 await self.stream_future_factories_data(analytics_records, topic, streaming_rate_hz=5.0)
 elif topic == 'future-factories-ml':
 ml_records = self._prepare_ml_features(records)
 await self.stream_future_factories_data(ml_records, topic, streaming_rate_hz=2.0)
 else:
 await self.stream_future_factories_data(records, topic, streaming_rate_hz=10.0)
 
 await asyncio.sleep(1) # Brief pause between topics
 
 def _prepare_analytics_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
 """Prepare data for real-time analytics"""
 analytics_records = []
 
 for record in records:
 sensor_data = record['sensor_data']
 
 # Calculate derived metrics
 avg_temp = np.mean([
 sensor_data.get('Q_VFD1_Temperature', 0),
 sensor_data.get('Q_VFD2_Temperature', 0),
 sensor_data.get('Q_VFD3_Temperature', 0),
 sensor_data.get('Q_VFD4_Temperature', 0)
 ])
 
 avg_speed = np.mean([
 sensor_data.get('M_Conv1_Speed_mmps', 0),
 sensor_data.get('M_Conv2_Speed_mmps', 0),
 sensor_data.get('M_Conv3_Speed_mmps', 0),
 sensor_data.get('M_Conv4_Speed_mmps', 0)
 ])
 
 analytics_record = {
 **record,
 'analytics_features': {
 'avg_temperature': avg_temp,
 'avg_conveyor_speed': avg_speed,
 'total_images': len(record['image_references']),
 'data_quality_score': self._calculate_data_quality(sensor_data)
 }
 }
 
 analytics_records.append(analytics_record)
 
 return analytics_records
 
 def _prepare_ml_features(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
 """Prepare feature-engineered data for ML"""
 ml_records = []
 
 for record in records:
 sensor_data = record['sensor_data']
 
 # Feature engineering
 features = {
 'temperature_variance': np.var([
 sensor_data.get('Q_VFD1_Temperature', 0),
 sensor_data.get('Q_VFD2_Temperature', 0),
 sensor_data.get('Q_VFD3_Temperature', 0),
 sensor_data.get('Q_VFD4_Temperature', 0)
 ]),
 'speed_efficiency_ratio': self._calculate_efficiency_ratio(sensor_data),
 'anomaly_indicators': self._extract_anomaly_features(sensor_data),
 'temporal_features': {
 'hour_of_day': datetime.fromisoformat(record['original_timestamp']).hour,
 'is_weekend': datetime.fromisoformat(record['original_timestamp']).weekday() >= 5
 }
 }
 
 ml_record = {
 **record,
 'ml_features': features,
 'feature_version': '1.0',
 'model_ready': True
 }
 
 ml_records.append(ml_record)
 
 return ml_records
 
 def _calculate_data_quality(self, sensor_data: Dict[str, Any]) -> float:
 """Calculate data quality score"""
 total_fields = len(sensor_data)
 non_null_fields = sum(1 for v in sensor_data.values() if v is not None and v != 0)
 return non_null_fields / total_fields if total_fields > 0 else 0.0
 
 def _calculate_efficiency_ratio(self, sensor_data: Dict[str, Any]) -> float:
 """Calculate operational efficiency ratio"""
 speeds = [
 sensor_data.get('M_Conv1_Speed_mmps', 0),
 sensor_data.get('M_Conv2_Speed_mmps', 0),
 sensor_data.get('M_Conv3_Speed_mmps', 0),
 sensor_data.get('M_Conv4_Speed_mmps', 0)
 ]
 
 avg_speed = np.mean(speeds)
 theoretical_max = 150.0 # Assume max speed
 return min(avg_speed / theoretical_max, 1.0) if theoretical_max > 0 else 0.0
 
 def _extract_anomaly_features(self, sensor_data: Dict[str, Any]) -> Dict[str, Any]:
 """Extract features that might indicate anomalies"""
 temps = [
 sensor_data.get('Q_VFD1_Temperature', 0),
 sensor_data.get('Q_VFD2_Temperature', 0),
 sensor_data.get('Q_VFD3_Temperature', 0),
 sensor_data.get('Q_VFD4_Temperature', 0)
 ]
 
 return {
 'temp_over_90': sum(1 for t in temps if t > 90),
 'temp_under_70': sum(1 for t in temps if t < 70),
 'temp_variance_high': np.var(temps) > 25,
 'safety_door_issues': sensor_data.get('I_SafetyDoor1_Status', 1) == 0 or 
 sensor_data.get('I_SafetyDoor2_Status', 1) == 0
 }
 
 def _delivery_callback(self, err, msg):
 """Kafka delivery callback"""
 if err:
 logger.error(f"Message delivery failed: {err}")
 else:
 logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}]")
 
 def get_metrics(self) -> Dict[str, Any]:
 """Get streaming metrics"""
 elapsed_time = time.time() - self.metrics['start_time']
 
 return {
 **self.metrics,
 'elapsed_time_seconds': elapsed_time,
 'records_per_second': self.metrics['records_processed'] / max(elapsed_time, 1),
 'bytes_per_second': self.metrics['bytes_sent'] / max(elapsed_time, 1),
 'gcp_integration': 'active'
 }
 
 def close(self):
 """Clean shutdown"""
 logger.info("Shutting down GCP Kafka producer...")
 self.producer.flush(timeout=30)
 self.producer = None

# Main demonstration
async def demonstrate_gcp_streaming():
 """Demonstrate GCP-integrated streaming with Future Factories data"""
 
 logger.info(f"Starting GCP-Integrated Future Factories Streaming Demo")
 logger.info("=" * 60)
 
 # Initialize producer
 producer = GCPKafkaProducer(
 gcs_bucket="resolve-agent-pipeline-resolve-binaries",
 project_id="resolve-agent-pipeline"
 )
 
 try:
 # Step 1: Read real Future Factories data from GCS
 logger.info(f"Step 1: Reading Future Factories data from GCS...")
 records = producer.read_future_factories_data(["data_31000.json"])
 
 if not records:
 logger.error(" No data found - check GCS bucket access")
 return
 
 logger.info(f"Loaded {len(records)} records from Future Factories dataset")
 
 # Step 2: Stream to single topic for basic demonstration
 logger.info(f"Step 2: Streaming to Kafka...")
 await producer.stream_future_factories_data(
 records[:100], # Limit for demo
 topic="future-factories-demo",
 streaming_rate_hz=20.0 # 20 Hz for demo
 )
 
 # Step 3: Show GCP integration with multiple topics
 logger.info(f"Step 3: GCP Analytics Integration...")
 await producer.stream_with_gcp_analytics_integration(records[:50])
 
 # Step 4: Display metrics
 logger.info(f"Step 4: Streaming Metrics")
 metrics = producer.get_metrics()
 for key, value in metrics.items():
 logger.info(f"{key}: {value}")
 
 logger.info(f"GCP Streaming demonstration complete!")
 
 except Exception as e:
 logger.error(f"Demo failed: {e}")
 finally:
 producer.close()

if __name__ == "__main__":
 asyncio.run(demonstrate_gcp_streaming())
