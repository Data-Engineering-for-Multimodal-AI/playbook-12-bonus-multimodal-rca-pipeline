#!/usr/bin/env python3
"""
Demo script for Advanced Data Engineering Pipeline
Simulates the complete pipeline without requiring Docker infrastructure
"""

import asyncio
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, List
import sys
import random
from dataclasses import dataclass, asdict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class SimulatedEvent:
 """Simulated event for demonstration"""
 timestamp: str
 event_type: str
 data: Dict[str, Any]
 processed_by: str
 latency_ms: float

class MockKafkaProducer:
 """Mock Kafka producer for demonstration"""
 
 def __init__(self, topic: str):
 self.topic = topic
 self.messages_sent = 0
 self.events_buffer = []
 
 async def send_event(self, event: Dict[str, Any]) -> bool:
 """Simulate sending event to Kafka"""
 await asyncio.sleep(0.01) # Simulate network latency
 
 self.events_buffer.append(event)
 self.messages_sent += 1
 
 logger.info(f"Sent to {self.topic}: {event.get('event_type', 'unknown')} "
 f"(total: {self.messages_sent})")
 return True
 
 def get_metrics(self) -> Dict[str, Any]:
 return {
 'topic': self.topic,
 'messages_sent': self.messages_sent,
 'buffer_size': len(self.events_buffer)
 }

class MockFlinkProcessor:
 """Mock Flink stream processor for demonstration"""
 
 def __init__(self):
 self.processed_events = 0
 self.anomalies_detected = 0
 self.processing_times = []
 
 async def process_stream(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
 """Simulate real-time stream processing"""
 processed_events = []
 
 for event in events:
 start_time = time.time()
 
 # Simulate anomaly detection
 is_anomaly = self._detect_anomaly(event)
 
 # Simulate data enrichment
 enriched_event = {
 **event,
 'anomaly_detected': is_anomaly,
 'processing_timestamp': datetime.now().isoformat(),
 'processor_id': 'flink-taskmanager-1'
 }
 
 processing_time = (time.time() - start_time) * 1000
 self.processing_times.append(processing_time)
 
 processed_events.append(enriched_event)
 self.processed_events += 1
 
 if is_anomaly:
 self.anomalies_detected += 1
 logger.warning(f" Anomaly detected in {event.get('sensor_id', 'unknown')}")
 
 # Simulate processing delay
 await asyncio.sleep(0.005)
 
 avg_processing = sum(self.processing_times[-10:]) / min(10, len(self.processing_times))
 logger.info(f"Processed {len(events)} events, avg latency: {avg_processing:.2f}ms")
 
 return processed_events
 
 def _detect_anomaly(self, event: Dict[str, Any]) -> bool:
 """Simple anomaly detection simulation"""
 if event.get('event_type') == 'sensor_reading':
 value = event.get('data', {}).get('value', 0)
 # Simple threshold-based detection
 return value > 95.0 or value < 10.0
 return False
 
 def get_metrics(self) -> Dict[str, Any]:
 avg_latency = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
 return {
 'events_processed': self.processed_events,
 'anomalies_detected': self.anomalies_detected,
 'average_latency_ms': avg_latency,
 'throughput_events_per_sec': self.processed_events / max(1, len(self.processing_times) / 1000)
 }

class MockEdgeDevice:
 """Mock edge device for demonstration"""
 
 def __init__(self, device_id: str):
 self.device_id = device_id
 self.events_generated = 0
 self.compression_ratio = 0.75 # 75% compression
 
 async def generate_sensor_data(self) -> Dict[str, Any]:
 """Generate realistic sensor data"""
 base_temp = 85.0
 variation = random.gauss(0, 5) # Normal distribution with std=5
 
 # Occasionally inject anomalies
 if random.random() < 0.05: # 5% chance
 variation += random.choice([30, -30]) # Extreme values
 
 event = {
 'timestamp': datetime.now().isoformat(),
 'event_type': 'sensor_reading',
 'device_id': self.device_id,
 'data': {
 'sensor_id': f'{self.device_id}-temp-01',
 'sensor_type': 'temperature',
 'value': base_temp + variation,
 'unit': 'celsius',
 'quality': random.uniform(0.85, 0.99)
 },
 'edge_processed': True
 }
 
 self.events_generated += 1
 return event
 
 async def generate_image_data(self) -> Dict[str, Any]:
 """Generate image analysis data"""
 # Simulate defect detection
 has_defect = random.random() < 0.1 # 10% chance of defect
 
 event = {
 'timestamp': datetime.now().isoformat(),
 'event_type': 'image_analysis',
 'device_id': self.device_id,
 'data': {
 'camera_id': f'{self.device_id}-cam-01',
 'image_path': f'/images/{int(time.time())}.jpg',
 'defects_detected': 1 if has_defect else 0,
 'quality_score': 0.7 if has_defect else 0.95,
 'processing_time_ms': random.randint(30, 60)
 },
 'edge_processed': True
 }
 
 return event
 
 def get_metrics(self) -> Dict[str, Any]:
 return {
 'device_id': self.device_id,
 'events_generated': self.events_generated,
 'compression_ratio': self.compression_ratio,
 'status': 'active'
 }

class PipelineDemo:
 """Main demonstration orchestrator"""
 
 def __init__(self):
 self.kafka_producer = MockKafkaProducer("manufacturing-telemetry")
 self.flink_processor = MockFlinkProcessor()
 self.edge_devices = [
 MockEdgeDevice("factory-01-line-A"),
 MockEdgeDevice("factory-01-line-B"),
 MockEdgeDevice("factory-02-line-A")
 ]
 self.start_time = time.time()
 
 async def run_step_1_infrastructure_simulation(self):
 """Step 1: Simulate infrastructure startup"""
 logger.info("=" * 60)
 logger.info("STEP 1: Infrastructure Setup Simulation")
 logger.info("=" * 60)
 
 services = [
 "Zookeeper", "Kafka", "Schema Registry", "Redis", 
 "Flink JobManager", "Flink TaskManager", "MQTT Broker"
 ]
 
 for service in services:
 logger.info(f"Starting {service}...")
 await asyncio.sleep(0.5) # Simulate startup time
 logger.info(f"{service} ready")
 
 logger.info(f"All infrastructure services are running!")
 logger.info("")
 
 async def run_step_2_kafka_streaming(self):
 """Step 2: Demonstrate Kafka streaming"""
 logger.info("=" * 60)
 logger.info("STEP 2: Kafka Streaming Demonstration")
 logger.info("=" * 60)
 
 logger.info(f"Starting data ingestion from edge devices...")
 
 # Generate and send events from multiple edge devices
 for i in range(20):
 # Simulate concurrent data from multiple devices
 tasks = []
 for device in self.edge_devices:
 # Sensor data every iteration
 sensor_event = await device.generate_sensor_data()
 tasks.append(self.kafka_producer.send_event(sensor_event))
 
 # Image data every 5th iteration
 if i % 5 == 0:
 image_event = await device.generate_image_data()
 tasks.append(self.kafka_producer.send_event(image_event))
 
 # Send all events concurrently
 await asyncio.gather(*tasks)
 await asyncio.sleep(0.1) # 10 Hz sampling rate
 
 metrics = self.kafka_producer.get_metrics()
 logger.info(f"Kafka Metrics: {metrics}")
 logger.info("")
 
 async def run_step_3_flink_processing(self):
 """Step 3: Demonstrate Flink stream processing"""
 logger.info("=" * 60)
 logger.info("STEP 3: Flink Stream Processing")
 logger.info("=" * 60)
 
 # Process events in batches to simulate streaming windows
 events = self.kafka_producer.events_buffer
 batch_size = 10
 
 logger.info(f"Processing {len(events)} events in batches of {batch_size}")
 
 all_processed = []
 for i in range(0, len(events), batch_size):
 batch = events[i:i + batch_size]
 processed_batch = await self.flink_processor.process_stream(batch)
 all_processed.extend(processed_batch)
 
 # Simulate streaming delay
 await asyncio.sleep(0.2)
 
 metrics = self.flink_processor.get_metrics()
 logger.info(f"Flink Metrics: {metrics}")
 logger.info("")
 
 return all_processed
 
 async def run_step_4_edge_computing(self):
 """Step 4: Demonstrate edge computing capabilities"""
 logger.info("=" * 60)
 logger.info("STEP 4: Edge Computing Demonstration")
 logger.info("=" * 60)
 
 logger.info(f"Simulating edge device capabilities...")
 
 # Show edge device metrics
 for device in self.edge_devices:
 metrics = device.get_metrics()
 logger.info(f"Edge Device {metrics['device_id']}: "
 f"{metrics['events_generated']} events, "
 f"{metrics['compression_ratio']:.0%} compression")
 
 # Simulate edge ML inference
 logger.info(f"Running edge ML inference...")
 await asyncio.sleep(0.5)
 
 inference_results = {
 'models_loaded': 3,
 'inference_time_avg_ms': 25,
 'accuracy': 0.94,
 'memory_usage_mb': 145
 }
 
 logger.info(f"Edge ML Results: {inference_results}")
 logger.info("")
 
 async def run_step_5_observability(self):
 """Step 5: Show observability and metrics"""
 logger.info("=" * 60)
 logger.info("STEP 5: Observability & Monitoring")
 logger.info("=" * 60)
 
 elapsed_time = time.time() - self.start_time
 
 # Collect all metrics
 pipeline_metrics = {
 'pipeline_runtime_seconds': round(elapsed_time, 2),
 'total_events_processed': self.flink_processor.processed_events,
 'anomalies_detected': self.flink_processor.anomalies_detected,
 'kafka_throughput': self.kafka_producer.messages_sent / elapsed_time,
 'edge_devices_active': len(self.edge_devices),
 'data_quality_score': 0.96
 }
 
 logger.info(f"Overall Pipeline Metrics:")
 for metric, value in pipeline_metrics.items():
 logger.info(f"{metric}: {value}")
 
 # Simulate dashboard URLs
 logger.info("")
 logger.info(f"Observability Dashboard URLs:")
 logger.info(f"• Grafana: http://localhost:3000")
 logger.info(f"• Flink Dashboard: http://localhost:8082")
 logger.info(f"• Kafka Manager: http://localhost:9092")
 logger.info(f"• Prometheus: http://localhost:9090")
 logger.info("")
 
 async def run_complete_demo(self):
 """Run the complete pipeline demonstration"""
 logger.info(f"Starting Advanced Data Engineering Pipeline Demo")
 logger.info(f"Demonstrating enterprise-grade streaming, edge computing, and observability")
 logger.info("")
 
 # Run all steps
 await self.run_step_1_infrastructure_simulation()
 await self.run_step_2_kafka_streaming()
 processed_events = await self.run_step_3_flink_processing()
 await self.run_step_4_edge_computing()
 await self.run_step_5_observability()
 
 logger.info("=" * 60)
 logger.info(f"PIPELINE DEMONSTRATION COMPLETE!")
 logger.info("=" * 60)
 logger.info("")
 logger.info("Key Features Demonstrated:")
 logger.info("• Real-time streaming with Kafka-style event processing")
 logger.info("• Stream processing with Flink-style windowing and analytics")
 logger.info("• Edge computing with local ML inference simulation")
 logger.info("• Enterprise observability with comprehensive metrics")
 logger.info("• Multi-device coordination and data compression")
 logger.info("")
 logger.info(f"To run with real infrastructure:")
 logger.info("1. Start Docker Desktop")
 logger.info("2. Run: docker-compose up -d")
 logger.info("3. Execute: python streaming/kafka_producer.py")
 logger.info("4. Run: python streaming/flink_processor.py")

async def main():
 """Main entry point"""
 if len(sys.argv) > 1 and sys.argv[1] == "--step":
 step = int(sys.argv[2]) if len(sys.argv) > 2 else 1
 demo = PipelineDemo()
 
 if step == 1:
 await demo.run_step_1_infrastructure_simulation()
 elif step == 2:
 await demo.run_step_1_infrastructure_simulation()
 await demo.run_step_2_kafka_streaming()
 elif step == 3:
 await demo.run_step_1_infrastructure_simulation()
 await demo.run_step_2_kafka_streaming()
 await demo.run_step_3_flink_processing()
 elif step == 4:
 await demo.run_step_1_infrastructure_simulation()
 await demo.run_step_2_kafka_streaming()
 await demo.run_step_3_flink_processing()
 await demo.run_step_4_edge_computing()
 elif step >= 5:
 demo = PipelineDemo()
 await demo.run_complete_demo()
 else:
 # Run complete demo
 demo = PipelineDemo()
 await demo.run_complete_demo()

if __name__ == "__main__":
 asyncio.run(main())
