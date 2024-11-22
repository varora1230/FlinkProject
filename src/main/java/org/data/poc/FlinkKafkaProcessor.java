package org.data.poc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.data.pojo.KafkaMessage;
import org.data.pojo.Person;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

/**
 * FlinkKafkaProcessor is responsible for consuming messages from Kafka, processing them,
 * and producing results to Kafka and file system (Parquet format).
 */
public class FlinkKafkaProcessor {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // Enable checkpointing with 5 seconds delay

        String bootstrapServers = "localhost:9092";
        String inputTopic = "test";
        String consumerGroup = "flink-consumer-group";

        // Create Kafka source to consume messages
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest()) // Start consuming from the latest offset
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Use SimpleStringSchema to deserialize the value from Kafka messages
                .build();

        // Create a stream from the Kafka source with no watermark strategy
        DataStream<String> inputStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // Process the stream: parse the input message, calculate age and assign to "even" or "odd" topic
        DataStream<KafkaMessage> processedStream = inputStream
                .map(message -> {
                    try{
                        ObjectMapper obj = new ObjectMapper();
                        Person person = obj.readValue(message, Person.class);
                        // Calculate the person's age based on their date of birth
                        LocalDate dob = LocalDate.parse(person.getDob(), DateTimeFormatter.ofPattern("dd-MM-yyyy"));
                        int age = Period.between(dob, LocalDate.now()).getYears();
                        person.setAge(age);
                        // Assign a topic name based on whether the age is even or odd
                        String topicName = (age % 2 == 0) ? "even" : "odd";
                        return new KafkaMessage(topicName, obj.writeValueAsString(person));
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + message);
                        e.printStackTrace();
                        return null;
                    }
                });

        // Sink the processed stream to Kafka with dynamic topic routing
        KafkaSink<KafkaMessage> kafkaSink = KafkaSink.<KafkaMessage>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new KafkaMessageSerializationSchema())
                .build();

        // Actual Kafka Sinking...
        processedStream.sinkTo(kafkaSink);

        // Persist processed data to Parquet on the file system (HDFS or local)
        FileSink<KafkaMessage> parquetSink = FileSink
                .forBulkFormat(new Path("file:///tmp/flink/data/"), ParquetAvroWriters.forReflectRecord(KafkaMessage.class))
                .withBucketCheckInterval(1000)
                .withBucketAssigner(new KafkaMessageBucketAssigner()) // Assign KafkaMessage to Topic name buckets
                .build();

        // Actual File Sinking...
        processedStream.sinkTo(parquetSink);

        // Execute the Flink job
        env.execute("Flink Kafka Processor");
    }
}
