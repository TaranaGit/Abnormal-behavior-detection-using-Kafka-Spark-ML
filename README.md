# Abnormal-behavior-detection-using-Kafka-Spark-ML

This project introduces a real-time abnormal behavior detection platform developed using data streams from the E4 wristband, a wearable medical device. This platform facilitates the generation of streaming physiological signals captured by the wristband, which are then transmitted to a mobile application and subsequently captured by Kafka topics in real-time. Leveraging Spark streaming pipelines, we process these data streams and apply machine learning classification algorithms to detect both normal and abnormal behavior patterns. Upon detection of abnormality, notifications are promptly sent to designated recipients such as caregivers or medical professionals, enabling timely intervention when necessary. This project aims to enhance proactive healthcare monitoring, offering valuable insights and timely alerts for improved patient care.

* Kafka
    Kafka is an open-source event streaming platform primarily used for building real-time data pipelines and streaming applications. It is designed to handle high-throughput, fault-tolerant, and scalable data streaming by allowing producers to publish data to topics and consumers to subscribe to these topics and process the data in real-time. Kafka provides features such as durability, scalability, and distributed architecture, making it suitable for use cases like log aggregation, real-time analytics, and event-driven architectures.

* Spark
    Spark, on the other hand, is an open-source distributed computing system, designed to process large-scale data processing tasks with high speed and ease of use. Spark offers various modules, including Spark SQL for structured data processing, Spark Streaming for real-time data processing.

# Getting Started
    *  Command for running producer part: python .\ppg_producer.py   
    *  Command for running Consumer part:spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 .\ppg_consumer.py

# System Architecture
![Getting Started](image/systemArchitecture.PNG)

