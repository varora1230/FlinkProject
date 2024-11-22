package org.data.poc;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.data.pojo.KafkaMessage;

/**
 * Custom Kafka serialization schema for KafkaMessage.
 */
public class KafkaMessageSerializationSchema implements KafkaRecordSerializationSchema<KafkaMessage> {
    /**
     * Initializes the serialization schema. (Inherited from KafkaRecordSerializationSchema)
     * @param context The initialization context.
     * @param sinkContext The Kafka sink context.
     * @throws Exception If initialization fails.
     */
    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    /**
     * Serializes a KafkaMessage into a ProducerRecord to be sent to Kafka.
     * @param kafkaMessage The KafkaMessage to serialize.
     * @param kafkaSinkContext The context of the Kafka sink.
     * @param timestamp A timestamp for the record
     * @return The serialized ProducerRecord.
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaMessage kafkaMessage, KafkaSinkContext kafkaSinkContext, Long timestamp) {
        return new ProducerRecord<>(kafkaMessage.getTopicName(), null, kafkaMessage.getMessage().getBytes());
    }
}
