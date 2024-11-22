package org.data.poc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Utility class for Kafka operations: Producing and consuming messages.
 */
public class KafkaUtil {

    private final String bootstrapServers;

    public KafkaUtil(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Produces a message to a Kafka topic.
     *
     * @param topic   The topic to send the message to.
     * @param key     The key of the message (optional, can be null).
     * @param message The message to send.
     */
    public void produce(String topic, String key, String message) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Message sent to topic %s partition %d offset %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
    }

    /**
     * Consumes messages from a Kafka topic.
     *
     * @param topic        The topic to consume messages from.
     * @param groupId      The consumer group ID.
     * @param pollDuration The duration to poll for messages (in milliseconds).
     */
    public void consume(String topic, String groupId, long pollDuration) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Subscribed to topic: %s%n", topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollDuration));
                records.forEach(record -> {
                    System.out.printf("Received message: Key=%s, Value=%s, Topic=%s, Partition=%d, Offset=%d%n",
                            record.key(), record.value(), record.topic(), record.partition(), record.offset());
                });
            }
        }
    }

    public static void main(String[] args) {
        KafkaUtil kafkaUtil = new KafkaUtil("localhost:9092");

        int m = 0;
        while(m<1000) {
            kafkaUtil.produce("test", null, "{\"name\": \"shivani\",\"address\": \"Sydney\",\"dob\": \"01-01-2000\"}");
            kafkaUtil.produce("test", null, "{\"name\": \"Parishy\",\"address\": \"India\",\"dob\": \"01-01-2021\"}");
            m++;
        }

        // Consume messages
        kafkaUtil.consume("test", "test-kafka", 1000);
    }
}
