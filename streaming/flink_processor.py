"""
Apache Flink Stream Processor for Real-time Manufacturing Analytics
Demonstrates complex event processing, windowing, and real-time ML inference
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common import Types
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetectionFunction(KeyedProcessFunction):
    """
    Real-time anomaly detection using statistical methods and ML models
    Maintains state for each sensor and detects deviations from normal patterns
    """
    
    def __init__(self):
        # State to track sensor statistics
        self.sensor_stats_state = None
        self.anomaly_threshold = 3.0  # Standard deviations
        
    def open(self, runtime_context):
        # Initialize state descriptors
        stats_descriptor = ValueStateDescriptor(
            "sensor_stats",
            Types.MAP(Types.STRING(), Types.DOUBLE())
        )
        self.sensor_stats_state = runtime_context.get_state(stats_descriptor)
    
    def process_element(self, value, ctx, out):
        """Process each sensor reading for anomaly detection"""
        try:
            # Parse the incoming sensor data
            sensor_data = json.loads(value.value) if hasattr(value, 'value') else value
            
            sensor_id = sensor_data.get('sensor_id')
            current_value = float(sensor_data.get('value', 0))
            timestamp = sensor_data.get('timestamp')
            
            # Get current sensor statistics
            current_stats = self.sensor_stats_state.value()
            if current_stats is None:
                current_stats = {
                    'count': 0,
                    'sum': 0.0,
                    'sum_squares': 0.0,
                    'mean': 0.0,
                    'std': 0.0,
                    'min': float('inf'),
                    'max': float('-inf')
                }
            
            # Update statistics incrementally
            current_stats['count'] += 1
            current_stats['sum'] += current_value
            current_stats['sum_squares'] += current_value ** 2
            current_stats['min'] = min(current_stats['min'], current_value)
            current_stats['max'] = max(current_stats['max'], current_value)
            
            # Calculate running mean and standard deviation
            n = current_stats['count']
            current_stats['mean'] = current_stats['sum'] / n
            
            if n > 1:
                variance = (current_stats['sum_squares'] - (current_stats['sum'] ** 2) / n) / (n - 1)
                current_stats['std'] = variance ** 0.5 if variance > 0 else 0
            
            # Detect anomaly
            is_anomaly = False
            anomaly_score = 0.0
            
            if current_stats['std'] > 0 and n > 10:  # Need enough data points
                z_score = abs(current_value - current_stats['mean']) / current_stats['std']
                is_anomaly = z_score > self.anomaly_threshold
                anomaly_score = z_score / self.anomaly_threshold
            
            # Update state
            self.sensor_stats_state.update(current_stats)
            
            # Create enriched output
            enriched_data = {
                **sensor_data,
                'anomaly_detected': is_anomaly,
                'anomaly_score': anomaly_score,
                'sensor_mean': current_stats['mean'],
                'sensor_std': current_stats['std'],
                'data_points': n,
                'processing_timestamp': datetime.now().isoformat()
            }
            
            # Output to downstream
            out.collect(json.dumps(enriched_data))
            
            # Log significant anomalies
            if is_anomaly:
                logger.warning(f"Anomaly detected for {sensor_id}: value={current_value}, z_score={z_score:.2f}")
                
        except Exception as e:
            logger.error(f"Error processing sensor data: {e}")

class MultimodalFusionFunction(ProcessFunction):
    """
    Fuses sensor data with image analysis results based on temporal windows
    Implements late-arrival handling and correlation logic
    """
    
    def __init__(self, window_size_seconds=30):
        self.window_size = window_size_seconds
        self.sensor_buffer = {}
        self.image_buffer = {}
        
    def process_element(self, value, ctx, out):
        """Process and correlate multimodal data streams"""
        try:
            data = json.loads(value.value) if hasattr(value, 'value') else value
            timestamp = datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat()))
            
            # Determine data type and buffer accordingly
            if 'sensor_id' in data:
                self._buffer_sensor_data(data, timestamp)
            elif 'camera_id' in data:
                self._buffer_image_data(data, timestamp)
            
            # Attempt fusion for current time window
            fused_records = self._attempt_fusion(timestamp)
            
            # Output fused records
            for record in fused_records:
                out.collect(json.dumps(record))
                
        except Exception as e:
            logger.error(f"Error in multimodal fusion: {e}")
    
    def _buffer_sensor_data(self, data, timestamp):
        """Buffer sensor data with timestamp-based indexing"""
        window_key = self._get_window_key(timestamp)
        
        if window_key not in self.sensor_buffer:
            self.sensor_buffer[window_key] = []
        
        self.sensor_buffer[window_key].append(data)
        
        # Clean old windows
        self._cleanup_old_windows(timestamp)
    
    def _buffer_image_data(self, data, timestamp):
        """Buffer image analysis data"""
        window_key = self._get_window_key(timestamp)
        
        if window_key not in self.image_buffer:
            self.image_buffer[window_key] = []
        
        self.image_buffer[window_key].append(data)
    
    def _get_window_key(self, timestamp):
        """Generate window key for time-based bucketing"""
        window_start = timestamp.replace(second=0, microsecond=0)
        window_start = window_start.replace(
            minute=(window_start.minute // (self.window_size // 60)) * (self.window_size // 60)
        )
        return window_start.isoformat()
    
    def _attempt_fusion(self, current_timestamp):
        """Attempt to fuse sensor and image data for available time windows"""
        fused_records = []
        window_key = self._get_window_key(current_timestamp)
        
        # Check if we have both sensor and image data for this window
        if (window_key in self.sensor_buffer and 
            window_key in self.image_buffer and
            self.sensor_buffer[window_key] and 
            self.image_buffer[window_key]):
            
            # Create fused record
            fused_record = {
                'window_start': window_key,
                'fusion_timestamp': datetime.now().isoformat(),
                'sensor_data': self.sensor_buffer[window_key],
                'image_data': self.image_buffer[window_key],
                'sensor_count': len(self.sensor_buffer[window_key]),
                'image_count': len(self.image_buffer[window_key]),
                'correlation_strength': self._calculate_correlation(),
                'factory_id': self.sensor_buffer[window_key][0].get('factory_id', 'unknown')
            }
            
            fused_records.append(fused_record)
            
            # Clear processed data
            del self.sensor_buffer[window_key]
            del self.image_buffer[window_key]
        
        return fused_records
    
    def _calculate_correlation(self):
        """Calculate correlation strength between sensor and image data"""
        # Simplified correlation - in production, this would be more sophisticated
        return 0.85  # Placeholder
    
    def _cleanup_old_windows(self, current_timestamp):
        """Remove data older than processing window"""
        cutoff_time = current_timestamp - timedelta(minutes=5)
        
        windows_to_remove = [
            key for key in self.sensor_buffer.keys() 
            if datetime.fromisoformat(key) < cutoff_time
        ]
        
        for window_key in windows_to_remove:
            if window_key in self.sensor_buffer:
                del self.sensor_buffer[window_key]
            if window_key in self.image_buffer:
                del self.image_buffer[window_key]

class FlinkStreamProcessor:
    """
    Main Flink stream processing pipeline
    Orchestrates the entire real-time analytics workflow
    """
    
    def __init__(self, kafka_bootstrap_servers="localhost:9092"):
        self.kafka_servers = kafka_bootstrap_servers
        self.env = None
        self.table_env = None
        
    def setup_environment(self):
        """Initialize Flink execution environment"""
        # Create streaming environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(4)  # Adjust based on cluster size
        
        # Enable checkpointing for fault tolerance
        self.env.enable_checkpointing(60000)  # Checkpoint every minute
        
        # Create table environment for SQL operations
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.table_env = StreamTableEnvironment.create(self.env, settings)
        
        logger.info("Flink environment initialized")
    
    def create_kafka_source(self, topic, group_id="flink-consumer"):
        """Create Kafka source connector"""
        kafka_props = {
            'bootstrap.servers': self.kafka_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        
        # JSON schema for deserialization
        json_schema = Types.ROW_NAMED(
            ['value'],
            [Types.STRING()]
        )
        
        kafka_consumer = FlinkKafkaConsumer(
            topics=topic,
            deserialization_schema=JsonRowDeserializationSchema.builder()
                .type_info(json_schema)
                .build(),
            properties=kafka_props
        )
        
        return self.env.add_source(kafka_consumer)
    
    def create_kafka_sink(self, topic):
        """Create Kafka sink connector"""
        kafka_props = {
            'bootstrap.servers': self.kafka_servers,
        }
        
        json_schema = Types.ROW_NAMED(
            ['value'],
            [Types.STRING()]
        )
        
        kafka_producer = FlinkKafkaProducer(
            topic=topic,
            serialization_schema=JsonRowSerializationSchema.builder()
                .with_type_info(json_schema)
                .build(),
            producer_config=kafka_props
        )
        
        return kafka_producer
    
    def run_anomaly_detection_pipeline(self):
        """Run real-time anomaly detection on sensor streams"""
        
        # Source: Manufacturing telemetry
        sensor_stream = self.create_kafka_source("manufacturing-telemetry", "anomaly-detector")
        
        # Process: Anomaly detection
        anomaly_stream = (sensor_stream
                         .key_by(lambda x: json.loads(x.value)['sensor_id'])
                         .process(AnomalyDetectionFunction()))
        
        # Sink: Anomalies to alerts topic
        alerts_sink = self.create_kafka_sink("manufacturing-alerts")
        
        # Filter and send only anomalies
        (anomaly_stream
         .filter(lambda x: json.loads(x)['anomaly_detected'])
         .map(lambda x: Types.ROW_NAMED(['value'], [Types.STRING()])(x))
         .add_sink(alerts_sink))
        
        logger.info("Anomaly detection pipeline configured")
    
    def run_multimodal_fusion_pipeline(self):
        """Run multimodal data fusion pipeline"""
        
        # Sources
        sensor_stream = self.create_kafka_source("manufacturing-telemetry", "fusion-sensors")
        image_stream = self.create_kafka_source("manufacturing-images", "fusion-images")
        
        # Union streams for processing
        combined_stream = sensor_stream.union(image_stream)
        
        # Process: Multimodal fusion
        fused_stream = combined_stream.process(MultimodalFusionFunction())
        
        # Sink: Fused data to AI pipeline
        fused_sink = self.create_kafka_sink("manufacturing-fused")
        
        (fused_stream
         .map(lambda x: Types.ROW_NAMED(['value'], [Types.STRING()])(x))
         .add_sink(fused_sink))
        
        logger.info("Multimodal fusion pipeline configured")
    
    def create_streaming_sql_tables(self):
        """Create streaming SQL tables for analytics"""
        
        # Create sensor data table
        self.table_env.execute_sql("""
            CREATE TABLE sensor_data (
                timestamp STRING,
                factory_id STRING,
                line_id STRING,
                sensor_id STRING,
                sensor_type STRING,
                value DOUBLE,
                quality_score DOUBLE,
                anomaly_detected BOOLEAN,
                processing_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'manufacturing-telemetry',
                'properties.bootstrap.servers' = '{servers}',
                'properties.group.id' = 'sql-analytics',
                'format' = 'json'
            )
        """.format(servers=self.kafka_servers))
        
        # Create aggregated metrics table
        self.table_env.execute_sql("""
            CREATE TABLE sensor_metrics (
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                factory_id STRING,
                sensor_type STRING,
                avg_value DOUBLE,
                max_value DOUBLE,
                min_value DOUBLE,
                anomaly_count BIGINT,
                total_readings BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'manufacturing-metrics',
                'properties.bootstrap.servers' = '{servers}',
                'format' = 'json'
            )
        """.format(servers=self.kafka_servers))
        
        logger.info("Streaming SQL tables created")
    
    def run_sql_analytics(self):
        """Run continuous SQL analytics on streaming data"""
        
        # Real-time aggregations with tumbling windows
        analytics_query = """
            INSERT INTO sensor_metrics
            SELECT 
                TUMBLE_START(processing_time, INTERVAL '1' MINUTE) as window_start,
                TUMBLE_END(processing_time, INTERVAL '1' MINUTE) as window_end,
                factory_id,
                sensor_type,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value,
                SUM(CASE WHEN anomaly_detected THEN 1 ELSE 0 END) as anomaly_count,
                COUNT(*) as total_readings
            FROM sensor_data
            GROUP BY 
                TUMBLE(processing_time, INTERVAL '1' MINUTE),
                factory_id,
                sensor_type
        """
        
        self.table_env.execute_sql(analytics_query)
        logger.info("SQL analytics pipeline started")
    
    def execute(self):
        """Execute the complete streaming pipeline"""
        try:
            self.setup_environment()
            
            # Start all pipelines
            self.run_anomaly_detection_pipeline()
            self.run_multimodal_fusion_pipeline()
            self.create_streaming_sql_tables()
            self.run_sql_analytics()
            
            # Execute the job
            logger.info("Starting Flink streaming job...")
            self.env.execute("Manufacturing Stream Processing Pipeline")
            
        except Exception as e:
            logger.error(f"Error executing Flink pipeline: {e}")
            raise

# Main execution
if __name__ == "__main__":
    processor = FlinkStreamProcessor(kafka_bootstrap_servers="localhost:9092")
    processor.execute()
