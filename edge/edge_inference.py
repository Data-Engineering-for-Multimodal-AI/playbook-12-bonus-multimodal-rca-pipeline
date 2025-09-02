"""
Edge Computing Module for Real-time Manufacturing Intelligence
Demonstrates local ML inference, data compression, and intelligent sampling
"""

import asyncio
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import numpy as np
import cv2
from collections import deque
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
import redis
import paho.mqtt.client as mqtt

# Mock ML models for demonstration
class EdgeMLModel:
    """Lightweight ML model for edge inference"""
    
    def __init__(self, model_type: str):
        self.model_type = model_type
        self.version = "1.0.0"
        self.loaded_at = datetime.now()
        
    def predict_anomaly(self, sensor_data: Dict[str, float]) -> Dict[str, Any]:
        """Predict anomaly from sensor data"""
        # Simplified anomaly detection
        temp_threshold = 90.0
        is_anomaly = sensor_data.get('temperature', 0) > temp_threshold
        
        return {
            'is_anomaly': is_anomaly,
            'confidence': 0.95 if is_anomaly else 0.85,
            'model_version': self.version,
            'inference_time_ms': 15
        }
    
    def analyze_image(self, image_data: np.ndarray) -> Dict[str, Any]:
        """Analyze image for defects"""
        # Simplified image analysis
        # In production, this would use TensorRT, ONNX, or similar
        
        # Mock defect detection
        defects = []
        if np.random.random() < 0.1:  # 10% chance of defect
            defects.append({
                'type': 'scratch',
                'location': [100, 200, 150, 250],
                'confidence': 0.92
            })
        
        return {
            'defects': defects,
            'quality_score': 0.98 if not defects else 0.75,
            'processing_time_ms': 45,
            'image_hash': hashlib.md5(image_data.tobytes()).hexdigest()[:16]
        }

@dataclass
class EdgeEvent:
    """Structured event for edge processing"""
    timestamp: str
    device_id: str
    event_type: str
    data: Dict[str, Any]
    edge_processed: bool
    priority: int  # 1=critical, 2=high, 3=normal, 4=low

class IntelligentSampler:
    """Intelligent data sampling to reduce bandwidth usage"""
    
    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self.data_buffer = deque(maxlen=max_queue_size)
        self.last_significant_event = None
        self.baseline_stats = {}
        
    def should_transmit(self, event: EdgeEvent) -> bool:
        """Decide whether to transmit event based on significance"""
        
        # Always transmit critical events
        if event.priority == 1:
            return True
        
        # For sensor data, check for significant changes
        if event.event_type == 'sensor_reading':
            return self._is_significant_change(event)
        
        # For images, sample based on defect detection
        if event.event_type == 'image_analysis':
            return event.data.get('defects', []) or np.random.random() < 0.1
        
        # Default sampling rate
        return np.random.random() < 0.5
    
    def _is_significant_change(self, event: EdgeEvent) -> bool:
        """Determine if sensor reading represents significant change"""
        sensor_id = event.data.get('sensor_id')
        value = event.data.get('value', 0)
        
        if sensor_id not in self.baseline_stats:
            self.baseline_stats[sensor_id] = {
                'values': deque(maxlen=100),
                'mean': 0,
                'std': 0
            }
            return True  # First reading is always significant
        
        stats = self.baseline_stats[sensor_id]
        stats['values'].append(value)
        
        if len(stats['values']) > 10:
            values_array = np.array(stats['values'])
            stats['mean'] = np.mean(values_array)
            stats['std'] = np.std(values_array)
            
            # Check if current value is more than 2 standard deviations away
            if stats['std'] > 0:
                z_score = abs(value - stats['mean']) / stats['std']
                return z_score > 2.0
        
        return True  # Not enough data for statistical comparison

class EdgeCompressionEngine:
    """Compress data before transmission to reduce bandwidth"""
    
    @staticmethod
    def compress_sensor_data(sensor_events: List[EdgeEvent]) -> Dict[str, Any]:
        """Compress multiple sensor readings into summary statistics"""
        if not sensor_events:
            return {}
        
        # Group by sensor type
        grouped_data = {}
        for event in sensor_events:
            sensor_type = event.data.get('sensor_type', 'unknown')
            if sensor_type not in grouped_data:
                grouped_data[sensor_type] = []
            grouped_data[sensor_type].append(event.data.get('value', 0))
        
        # Create compressed summary
        compressed = {
            'timestamp_start': sensor_events[0].timestamp,
            'timestamp_end': sensor_events[-1].timestamp,
            'device_id': sensor_events[0].device_id,
            'compressed': True,
            'data_points': len(sensor_events),
            'summaries': {}
        }
        
        for sensor_type, values in grouped_data.items():
            values_array = np.array(values)
            compressed['summaries'][sensor_type] = {
                'mean': float(np.mean(values_array)),
                'std': float(np.std(values_array)),
                'min': float(np.min(values_array)),
                'max': float(np.max(values_array)),
                'count': len(values),
                'trend': 'stable'  # Simplified trend analysis
            }
        
        return compressed
    
    @staticmethod
    def compress_image_metadata(image_events: List[EdgeEvent]) -> Dict[str, Any]:
        """Compress image analysis results"""
        if not image_events:
            return {}
        
        total_defects = sum(len(event.data.get('defects', [])) for event in image_events)
        avg_quality = np.mean([event.data.get('quality_score', 1.0) for event in image_events])
        
        return {
            'timestamp_start': image_events[0].timestamp,
            'timestamp_end': image_events[-1].timestamp,
            'device_id': image_events[0].device_id,
            'compressed': True,
            'images_analyzed': len(image_events),
            'total_defects_detected': total_defects,
            'average_quality_score': float(avg_quality),
            'defect_types': list(set(
                defect.get('type') 
                for event in image_events 
                for defect in event.data.get('defects', [])
            ))
        }

class EdgeAgent:
    """Main edge computing agent for manufacturing intelligence"""
    
    def __init__(
        self,
        device_id: str,
        mqtt_broker: str = "localhost",
        redis_host: str = "localhost",
        max_workers: int = 4
    ):
        self.device_id = device_id
        self.mqtt_broker = mqtt_broker
        self.redis_host = redis_host
        self.max_workers = max_workers
        
        # Initialize components
        self.ml_model = EdgeMLModel("manufacturing-v1")
        self.sampler = IntelligentSampler()
        self.compression_engine = EdgeCompressionEngine()
        
        # Initialize connections
        self.mqtt_client = mqtt.Client()
        self.redis_client = redis.Redis(host=redis_host, decode_responses=True)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Event buffers for batching
        self.sensor_buffer = deque(maxlen=100)
        self.image_buffer = deque(maxlen=50)
        self.transmission_buffer = deque(maxlen=200)
        
        # Metrics
        self.metrics = {
            'events_processed': 0,
            'events_transmitted': 0,
            'compression_ratio': 0.0,
            'average_inference_time': 0.0,
            'last_transmission': None
        }
        
        self.setup_connections()
        
        logger = logging.getLogger(__name__)
        logger.info(f"Edge agent {device_id} initialized")
    
    def setup_connections(self):
        """Setup MQTT and Redis connections"""
        
        # MQTT callbacks
        def on_connect(client, userdata, flags, rc):
            logging.info(f"MQTT connected with result code {rc}")
            client.subscribe(f"factory/{self.device_id}/sensors/+")
            client.subscribe(f"factory/{self.device_id}/cameras/+")
        
        def on_message(client, userdata, msg):
            asyncio.create_task(self.handle_incoming_message(msg))
        
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_message = on_message
        self.mqtt_client.connect(self.mqtt_broker, 1883, 60)
        self.mqtt_client.loop_start()
    
    async def handle_incoming_message(self, msg):
        """Handle incoming MQTT messages"""
        try:
            topic_parts = msg.topic.split('/')
            device_type = topic_parts[-2]  # sensors or cameras
            device_instance = topic_parts[-1]
            
            data = json.loads(msg.payload.decode())
            
            if device_type == 'sensors':
                await self.process_sensor_data(device_instance, data)
            elif device_type == 'cameras':
                await self.process_camera_data(device_instance, data)
                
        except Exception as e:
            logging.error(f"Error handling message: {e}")
    
    async def process_sensor_data(self, sensor_id: str, data: Dict[str, Any]):
        """Process sensor data with edge ML inference"""
        
        # Run ML inference
        inference_result = await asyncio.get_event_loop().run_in_executor(
            self.executor, self.ml_model.predict_anomaly, data
        )
        
        # Create edge event
        event = EdgeEvent(
            timestamp=datetime.now().isoformat(),
            device_id=self.device_id,
            event_type='sensor_reading',
            data={
                **data,
                'sensor_id': sensor_id,
                'inference_result': inference_result,
                'edge_processed_at': datetime.now().isoformat()
            },
            edge_processed=True,
            priority=1 if inference_result['is_anomaly'] else 3
        )
        
        # Add to buffer and decide on transmission
        self.sensor_buffer.append(event)
        
        if self.sampler.should_transmit(event):
            self.transmission_buffer.append(event)
        
        # Cache locally for offline resilience
        await self.cache_event_locally(event)
        
        self.metrics['events_processed'] += 1
        
        # Log critical events
        if event.priority == 1:
            logging.warning(f"Critical anomaly detected: {inference_result}")
    
    async def process_camera_data(self, camera_id: str, data: Dict[str, Any]):
        """Process camera/image data with edge vision models"""
        
        # Simulate image loading (in production, load from data['image_path'])
        mock_image = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        
        # Run image analysis
        analysis_result = await asyncio.get_event_loop().run_in_executor(
            self.executor, self.ml_model.analyze_image, mock_image
        )
        
        # Create edge event
        event = EdgeEvent(
            timestamp=datetime.now().isoformat(),
            device_id=self.device_id,
            event_type='image_analysis',
            data={
                **data,
                'camera_id': camera_id,
                'analysis_result': analysis_result,
                'edge_processed_at': datetime.now().isoformat()
            },
            edge_processed=True,
            priority=1 if analysis_result['defects'] else 4
        )
        
        self.image_buffer.append(event)
        
        if self.sampler.should_transmit(event):
            self.transmission_buffer.append(event)
        
        await self.cache_event_locally(event)
        
        self.metrics['events_processed'] += 1
    
    async def cache_event_locally(self, event: EdgeEvent):
        """Cache events locally for offline resilience"""
        try:
            cache_key = f"edge_event:{self.device_id}:{int(time.time())}"
            event_data = asdict(event)
            self.redis_client.setex(
                cache_key, 
                3600,  # 1 hour TTL
                json.dumps(event_data, default=str)
            )
        except Exception as e:
            logging.error(f"Failed to cache event: {e}")
    
    async def periodic_transmission(self):
        """Periodically transmit batched and compressed data"""
        while True:
            try:
                await asyncio.sleep(30)  # Transmit every 30 seconds
                
                if self.transmission_buffer:
                    await self.transmit_batched_data()
                    
            except Exception as e:
                logging.error(f"Error in periodic transmission: {e}")
    
    async def transmit_batched_data(self):
        """Transmit batched and compressed data to cloud"""
        
        # Separate events by type
        sensor_events = [e for e in self.transmission_buffer if e.event_type == 'sensor_reading']
        image_events = [e for e in self.transmission_buffer if e.event_type == 'image_analysis']
        
        transmitted_count = 0
        
        # Transmit compressed sensor data
        if sensor_events:
            compressed_sensors = self.compression_engine.compress_sensor_data(sensor_events)
            await self.publish_to_cloud('manufacturing/sensors/compressed', compressed_sensors)
            transmitted_count += len(sensor_events)
        
        # Transmit compressed image metadata
        if image_events:
            compressed_images = self.compression_engine.compress_image_metadata(image_events)
            await self.publish_to_cloud('manufacturing/images/compressed', compressed_images)
            transmitted_count += len(image_events)
        
        # Transmit critical events individually
        critical_events = [e for e in self.transmission_buffer if e.priority == 1]
        for event in critical_events:
            await self.publish_to_cloud('manufacturing/alerts', asdict(event))
        
        # Update metrics
        self.metrics['events_transmitted'] += transmitted_count
        self.metrics['last_transmission'] = datetime.now().isoformat()
        
        # Calculate compression ratio
        original_size = len(self.transmission_buffer)
        compressed_size = len(compressed_sensors) + len(compressed_images) + len(critical_events)
        if original_size > 0:
            self.metrics['compression_ratio'] = 1 - (compressed_size / original_size)
        
        # Clear transmission buffer
        self.transmission_buffer.clear()
        
        logging.info(f"Transmitted {transmitted_count} events with {self.metrics['compression_ratio']:.2%} compression")
    
    async def publish_to_cloud(self, topic: str, data: Dict[str, Any]):
        """Publish data to cloud via MQTT"""
        try:
            message = json.dumps(data, default=str)
            self.mqtt_client.publish(f"cloud/{topic}", message)
        except Exception as e:
            logging.error(f"Failed to publish to cloud: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get edge agent performance metrics"""
        return {
            **self.metrics,
            'buffer_sizes': {
                'sensor_buffer': len(self.sensor_buffer),
                'image_buffer': len(self.image_buffer),
                'transmission_buffer': len(self.transmission_buffer)
            },
            'model_info': {
                'type': self.ml_model.model_type,
                'version': self.ml_model.version,
                'loaded_at': self.ml_model.loaded_at.isoformat()
            }
        }
    
    async def start(self):
        """Start the edge agent"""
        logging.info(f"Starting edge agent {self.device_id}")
        
        # Start periodic transmission
        transmission_task = asyncio.create_task(self.periodic_transmission())
        
        try:
            await transmission_task
        except KeyboardInterrupt:
            logging.info("Edge agent stopping...")
            transmission_task.cancel()
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self.executor.shutdown(wait=True)

# Example usage
async def simulate_edge_device():
    """Simulate an edge device with sensors and cameras"""
    
    edge_agent = EdgeAgent(device_id="edge-001")
    
    # Simulate sensor data
    async def generate_sensor_data():
        while True:
            sensor_data = {
                'temperature': 85.0 + np.random.normal(0, 5),
                'pressure': 100.0 + np.random.normal(0, 2),
                'vibration': 0.5 + np.random.normal(0, 0.1),
                'timestamp': datetime.now().isoformat()
            }
            
            await edge_agent.process_sensor_data("temp-001", sensor_data)
            await asyncio.sleep(1)  # 1 Hz sampling
    
    # Simulate camera data
    async def generate_camera_data():
        while True:
            camera_data = {
                'image_path': f'/images/frame_{int(time.time())}.jpg',
                'resolution': '640x480',
                'timestamp': datetime.now().isoformat()
            }
            
            await edge_agent.process_camera_data("cam-001", camera_data)
            await asyncio.sleep(5)  # 0.2 Hz for images
    
    # Start simulation
    await asyncio.gather(
        edge_agent.start(),
        generate_sensor_data(),
        generate_camera_data()
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(simulate_edge_device())
