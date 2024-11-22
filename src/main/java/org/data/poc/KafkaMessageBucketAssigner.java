package org.data.poc;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.data.pojo.KafkaMessage;

/**
 * Custom BucketAssigner for Flink FileSink that assigns each KafkaMessage to a bucket
 * based on its topic name. Useful for partitioning output files by topic.
 */
public class KafkaMessageBucketAssigner implements BucketAssigner<KafkaMessage, String> {
    /**
     * Assigns a bucket ID based on the Kafka message's topic name.
     *
     * @param km  The KafkaMessage being processed.
     * @param ctx Context of the record being processed.
     * @return The bucket ID derived from the Kafka message's topic name.
     */
    @Override
    public String getBucketId(KafkaMessage km, Context ctx) {
        // Use the topic name as the partition/bucket directory
        return km.getTopicName();
    }

    /**
     * Provides a serializer for the bucket ID.
     *
     * @return A serializer for bucket IDs.
     */
    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return null;
    }
}
