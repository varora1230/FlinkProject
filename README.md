Apache Flink Project: Dynamic Kafka Topic Routing and Parquet File Storage

This project demonstrates how to use Apache Flink to consume messages from a Kafka topic, process the data to calculate the age, dynamically route messages to separate Kafka topics (even-topic or odd-topic) based on the age, and finally store the messages in a Parquet file partitioned by the topic name.

Table of Contents
1. Project Overview
2. Prerequisites
3. Project Architecture
4. Setup Instructions
5. Code Workflow
6. Build and Run
7. Validation


Project Overview

The Flink job performs the following tasks:Consumes JSON messages from an input Kafka topic (input-topic).Deserializes the message into a Person object.Calculates the age from the dob (date of birth).Routes the message dynamically to Kafka topics:odd-topic: For persons with an odd age.even-topic: For persons with an even age.Writes the processed messages into Parquet files, partitioned by odd and even topics.

Prerequisites
1. Apache Flink: Version 1.17 or higher
2. Apache Kafka: Version 3.2 or higher
3. Hadoop HDFS (Optional): For storing Parquet files (local file system also supported).
4. Java Development Kit (JDK): Version 8 or higher
5. Maven: For building the project

Project Architecture
1. Input: JSON messages in the input-topic (Kafka)
2. Processing:Calculate the age from the dob.Route messages to odd-topic or even-topic based on the age.
3. Output:Publish messages to Kafka topics (odd-topic, even-topic).
4. Store all messages in Parquet format, partitioned by odd and even.

Setup Instructions
1. Kafka Setup
   kafka-topics.sh --create --bootstrap-server localhost:9092 --topic input-topic
   kafka-topics.sh --create --bootstrap-server localhost:9092 --topic odd-topic
   kafka-topics.sh --create --bootstrap-server localhost:9092 --topic even-topic
2. Configure Parquet Output Path
   Update the Flink code with your desired Parquet file output directory:
   String outputPath = "file:///path/to/output/directory";
3. Build the ProjectUse Maven to build the project:
   mvn clean package

Code Workflow
1. Kafka Source:
    Reads data from input-topic using KafkaSource.
2. Stream Processing:
    Deserializes JSON into Person objects.Computes age from the dob.
    Routes the message to either odd-topic or even-topic based on the age.
3. Kafka Sink:
    Publishes messages to respective Kafka topics using KafkaSink.
4. Parquet Sink:
    Writes all messages to Parquet files, partitioned by topic (odd or even).

Build and Run
1. Build the Flink JAR
    mvn clean package
    The JAR file will be available in the target directory.
2. Submit the Job
    Run the Flink job using the Flink CLI:
    bin/flink run -c org.data.poc.FlinkKafkaProcessor /Users/shivaniarora/IdeaProjects/FlinkProject/target/DataProcessor-1.0-SNAPSHOT.jar
   
Validation
1. Kafka Output Validation
    Consume messages from the odd-topic and even-topic using the Kafka CLI:
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic odd-topic --from-beginning
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic even-topic --from-beginning
2. Parquet File ValidationVerify the Parquet files:
    Check the output directory for Parquet files partitioned into odd and even folders.

