# Advanced Data Engineering Pipeline Tutorial

A step-by-step guide to setting up and running the enterprise-grade multimodal AI pipeline with real-time streaming and edge computing capabilities.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Infrastructure Deployment](#infrastructure-deployment)
4. [Pipeline Execution](#pipeline-execution)
5. [Production Deployment](#production-deployment)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Configuration](#advanced-configuration)

## Prerequisites

### System Requirements

**Minimum Requirements:**
- 8GB RAM
- 4 CPU cores
- 20GB free disk space
- Docker Desktop
- Python 3.9+

**Recommended for Production:**
- 32GB RAM
- 8+ CPU cores
- 100GB SSD storage
- Google Cloud SDK
- Kubernetes CLI (kubectl)

### Software Dependencies

1. **Docker Desktop**
   ```bash
   # Install Docker Desktop from: https://docker.com/desktop
   # Verify installation
   docker --version
   docker-compose --version
   ```

2. **Python Environment**
   ```bash
   # Install Python 3.9+
   python3 --version
   
   # Install pip packages
   pip install -r requirements.txt
   ```

3. **Google Cloud SDK (Optional)**
   ```bash
   # Install gcloud CLI
   curl https://sdk.cloud.google.com | bash
   
   # Initialize and authenticate
   gcloud init
   gcloud auth application-default login
   ```

## Environment Setup

### Step 1: Project Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/Data-Engineering-for-Multimodal-AI/pb-12-mm-endtoendpipeline.git
   cd pb-12-mm-endtoendpipeline/resolve-agent-bonus
   ```

2. **Verify Project Structure**
   ```bash
   tree -L 2
   ```
   
   Expected output:
   ```
   .
   ├── README.md
   ├── TUTORIAL.md
   ├── docker-compose.yml
   ├── cloud_demo.py
   ├── streaming/
   │   ├── kafka_producer.py
   │   ├── gcp_kafka_producer.py
   │   └── flink_processor.py
   ├── edge/
   │   └── edge_inference.py
   └── requirements.txt
   ```

### Step 2: Install Dependencies

1. **Create Virtual Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install Python Packages**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify Installation**
   ```bash
   python -c "import confluent_kafka; print('Kafka client installed')"
   python -c "import numpy; print('NumPy installed')"
   ```

## Infrastructure Deployment

### Step 3: Local Development Stack

1. **Start Docker Services**
   ```bash
   # Start all services in background
   docker-compose up -d
   
   # Expected output:
   # Creating network "resolve-agent-bonus_default" with the default driver
   # Creating zookeeper ... done
   # Creating kafka ... done
   # Creating schema-registry ... done
   # Creating redis ... done
   ```

2. **Verify Services**
   ```bash
   # Check service status
   docker-compose ps
   
   # Expected services running:
   # - zookeeper (port 2181)
   # - kafka (port 9092)
   # - schema-registry (port 8081)
   # - redis (port 6379)
   ```

3. **Wait for Services to Initialize**
   ```bash
   # Services need 30-60 seconds to fully start
   sleep 60
   
   # Test Kafka connectivity
   docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

### Step 4: Service Health Checks

1. **Kafka Health Check**
   ```bash
   # Create test topic
   docker-compose exec kafka kafka-topics --create \
     --topic test-topic \
     --bootstrap-server localhost:9092 \
     --partitions 3 \
     --replication-factor 1
   
   # List topics
   docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

2. **Schema Registry Health Check**
   ```bash
   # Check schema registry
   curl http://localhost:8081/subjects
   # Expected: []
   ```

3. **Redis Health Check**
   ```bash
   # Test Redis connection
   docker-compose exec redis redis-cli ping
   # Expected: PONG
   ```

## Pipeline Execution

### Step 5: Demo Pipeline

The pipeline demonstrates enterprise data engineering patterns using the Future Factories dataset.

#### Concept: Multi-Step Processing
The demo simulates a real manufacturing environment where:
- **Step 1**: Data ingestion from GCS (Google Cloud Storage)
- **Step 2**: Real-time streaming with Kafka
- **Step 3**: Complex event processing with Flink
- **Step 4**: Edge computing with local ML inference
- **Step 5**: Data lake analytics with BigQuery
- **Step 6**: Enterprise observability and monitoring

1. **Run Complete Demo**
   ```bash
   python cloud_demo.py
   ```
   
   **Expected Output:**
   ```
   2024-01-XX XX:XX:XX,XXX - INFO - ADVANCED DATA ENGINEERING PIPELINE DEMONSTRATION
   2024-01-XX XX:XX:XX,XXX - INFO - Using Real Future Factories Data + Enterprise GCP Architecture
   
   ======================================================================
   STEP 1: GCS Data Ingestion
   ======================================================================
   Description: Read multimodal Future Factories data from Cloud Storage
   GCP Services: Cloud Storage, IAM
   
   Accessing Future Factories dataset...
      File: data_31000.json - Manufacturing Cell Batch 31000
      File: data_32000.json - Manufacturing Cell Batch 32000
      File: BATCH31000/ - Image data (2000+ manufacturing images)
   ...
   Successfully ingested 287954 records
   Total: 30 hours of continuous manufacturing data
   ```

2. **Run Individual Steps**
   ```bash
   # Step 1: Data Ingestion
   python cloud_demo.py --step 1
   
   # Step 2: Kafka Streaming
   python cloud_demo.py --step 2
   
   # Step 3: Flink Processing
   python cloud_demo.py --step 3
   
   # Step 4: Edge Computing
   python cloud_demo.py --step 4
   
   # Step 5: Data Analytics
   python cloud_demo.py --step 5
   
   # Step 6: Observability
   python cloud_demo.py --step 6
   ```

### Step 6: Real-Time Streaming (Advanced)

For users with GCP access and the Future Factories dataset:

1. **Configure GCP Credentials**
   ```bash
   # Set up authentication
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
   export GOOGLE_CLOUD_PROJECT="your-project-id"
   ```

2. **Run GCP Kafka Producer**
   ```bash
   # Stream real data from GCS
   python streaming/gcp_kafka_producer.py
   ```
   
   **Expected Output:**
   ```
   2024-01-XX XX:XX:XX,XXX - INFO - Initialized GCP Kafka producer for bucket: resolve-agent-pipeline-resolve-binaries
   2024-01-XX XX:XX:XX,XXX - INFO - Reading datasets/future-factories/multimodal-2024-data/Batch 1/data_31000.json from GCS...
   2024-01-XX XX:XX:XX,XXX - INFO - Processed data_31000.json: 8743 records
   2024-01-XX XX:XX:XX,XXX - INFO - Starting stream to topic 'future-factories-demo' at 20.0 Hz
   ```

3. **Monitor Stream Processing**
   ```bash
   # In another terminal
   python streaming/flink_processor.py
   ```

## Production Deployment

### Step 7: Google Cloud Platform Setup

1. **Create GCP Project**
   ```bash
   # Create new project
   gcloud projects create your-pipeline-project
   
   # Set as default
   gcloud config set project your-pipeline-project
   
   # Enable required APIs
   gcloud services enable compute.googleapis.com
   gcloud services enable container.googleapis.com
   gcloud services enable bigquery.googleapis.com
   ```

2. **Set up BigQuery**
   ```bash
   # Create dataset
   bq mk --dataset your-project:manufacturing_data
   
   # Create tables
   bq mk --table your-project:manufacturing_data.sensor_readings schema_sensor.json
   bq mk --table your-project:manufacturing_data.image_analysis schema_images.json
   ```

3. **Deploy to Cloud Run**
   ```bash
   # Build and deploy streaming service
   gcloud run deploy streaming-processor \
     --source=./streaming \
     --region=us-central1 \
     --allow-unauthenticated
   ```

### Step 8: Kubernetes Deployment (Advanced)

1. **Create GKE Cluster**
   ```bash
   # Create cluster
   gcloud container clusters create pipeline-cluster \
     --num-nodes=3 \
     --machine-type=n1-standard-4 \
     --zone=us-central1-a
   
   # Get credentials
   gcloud container clusters get-credentials pipeline-cluster --zone=us-central1-a
   ```

2. **Deploy Applications**
   ```bash
   # Deploy Kafka (using Strimzi operator)
   kubectl apply -f k8s/kafka-cluster.yaml
   
   # Deploy Flink jobs
   kubectl apply -f k8s/flink-jobs.yaml
   
   # Deploy monitoring stack
   kubectl apply -f k8s/monitoring.yaml
   ```

## Troubleshooting

### Common Issues

1. **Docker Services Won't Start**
   ```bash
   # Check Docker daemon
   docker info
   
   # Restart Docker Desktop
   # Then try again:
   docker-compose down
   docker-compose up -d
   ```

2. **Port Conflicts**
   ```bash
   # Check what's using ports
   lsof -i :9092  # Kafka
   lsof -i :2181  # Zookeeper
   
   # Kill conflicting processes or modify docker-compose.yml ports
   ```

3. **Out of Memory Errors**
   ```bash
   # Increase Docker memory allocation in Docker Desktop settings
   # Minimum 6GB recommended, 8GB preferred
   
   # Check current allocation
   docker system info | grep Memory
   ```

4. **GCP Authentication Issues**
   ```bash
   # Re-authenticate
   gcloud auth application-default login
   
   # Verify credentials
   gcloud auth list
   ```

### Performance Issues

1. **Slow Kafka Performance**
   ```bash
   # Tune Kafka settings in docker-compose.yml
   # Increase:
   # - KAFKA_NUM_PARTITIONS
   # - KAFKA_DEFAULT_REPLICATION_FACTOR
   # - Memory allocation
   ```

2. **Flink Job Failures**
   ```bash
   # Check Flink logs
   docker-compose logs flink-jobmanager
   docker-compose logs flink-taskmanager
   
   # Increase parallelism in flink-conf.yaml
   ```

## Advanced Configuration

### Step 9: Custom Configuration

1. **Kafka Configuration**
   ```yaml
   # docker-compose.yml modifications
   kafka:
     environment:
       KAFKA_NUM_PARTITIONS: 12
       KAFKA_DEFAULT_REPLICATION_FACTOR: 3
       KAFKA_LOG_RETENTION_HOURS: 168
   ```

2. **Flink Configuration**
   ```yaml
   # flink-conf.yaml
   taskmanager.numberOfTaskSlots: 4
   parallelism.default: 8
   state.backend: rocksdb
   state.checkpoints.dir: file:///tmp/flink-checkpoints
   ```

3. **Monitoring Configuration**
   ```yaml
   # Add Prometheus metrics
   prometheus:
     image: prom/prometheus
     ports:
       - "9090:9090"
     volumes:
       - ./prometheus.yml:/etc/prometheus/prometheus.yml
   ```

### Step 10: Production Hardening

1. **Security Configuration**
   ```bash
   # Enable SSL for Kafka
   # Configure SASL authentication
   # Set up network policies in Kubernetes
   ```

2. **Data Governance**
   ```bash
   # Implement schema validation
   # Set up data lineage tracking
   # Configure retention policies
   ```

3. **Monitoring & Alerting**
   ```bash
   # Set up Grafana dashboards
   # Configure PagerDuty integration
   # Implement SLO monitoring
   ```

## Next Steps

1. **Explore the Code**: Review the streaming and edge computing implementations
2. **Customize for Your Data**: Adapt the pipeline for your specific use case
3. **Scale Up**: Deploy to production with proper monitoring and observability
4. **Contribute**: Share improvements and extensions with the community

## Support

- **Documentation**: Additional docs in the `/docs` folder
- **Issues**: Report bugs via GitHub Issues
- **Discussions**: Join community discussions for help and feature requests

For enterprise support and consulting, contact the Data Engineering for Multimodal AI team.
