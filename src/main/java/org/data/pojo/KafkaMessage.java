package org.data.pojo;

/**
 * KafkaMessage represents a message to be sent to Kafka with its associated topic.
 */
public class KafkaMessage {

    private String topicName;

    private String message;

    /**
     * Default constructor.
     */
    public KafkaMessage() {}

    /**
     * @param topicName The name of the Kafka topic.
     * @param message   The message content to be sent.
     */
    public KafkaMessage(String topicName, String message) {
        this.topicName = topicName;
        this.message = message;
    }

    /**
     * @return The topic name.
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * @param topicName The topic name to be set.
     */
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    /**
     * @return The message content.
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message The message content to be set.
     */
    public void setMessage(String message) {
        this.message = message;
    }
}
