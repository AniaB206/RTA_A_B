# jupyterlab
Python3.11 with Apache Kafka, Apache Spark and Redis for real time data analysis.

```bash
cd jupyterlab-project
docker compose up
```

## stop

```bash
ctrl + c 
docker compose down
```

### Contain: 
- Apache Kafka Broker
- Redis
- JupyterLab env with Apache Spark

### Python libraries
- flask==3.0.1 fastapi==0.115.5 for production env with python ML model 
- pennylane==0.39 - You can use it for quantum machine learning
- pandas==2.0.3 numpy==1.24.4 scipy==1.11.3 matplotlib==3.8.0 sympy==1.12 seaborn==0.13.0
- kafka-python==2.0.2 confluent_kafka==2.6.0
