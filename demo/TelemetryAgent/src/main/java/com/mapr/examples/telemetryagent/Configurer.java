package com.mapr.examples.telemetryagent;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Utility for loading properties
 */
public abstract class Configurer {
    public static final String STREAM_NAME = "stream.name";
    public static final String TOPIC_CARS_ALL = "topic.cars.all";
    public static final String TOPIC_CARS_SINGLE = "topic.cars.single";

    protected Properties props;
    public static final String KAFKA_PROPERTY_PREFIX = "kafka";

    public Configurer(String pathToProps) {
        this.props = new Properties();
        try {
            props.load(new FileInputStream(pathToProps));
        } catch (IOException e) {
            System.err.println("Could not read config file '" + pathToProps + "'!");
        }
    }

    public Properties getKafkaProps() {
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(getDefaults());
        props.entrySet().stream()
                .filter(kafkaProperty -> ((String) kafkaProperty.getKey())
                        .startsWith(KAFKA_PROPERTY_PREFIX))
                .forEach(kafkaProperty -> kafkaProps.put(((String) kafkaProperty.getKey())
                        .substring(KAFKA_PROPERTY_PREFIX.length() + 1), kafkaProperty.getValue()));
        kafkaProps.put("streams.consumer.default.stream", props.get(STREAM_NAME));

        for (Map.Entry<Object, Object> entry : kafkaProps.entrySet()) {
            System.out.println(String.format("%s = %s", entry.getKey().toString(), entry.getValue().toString()));
        }
        return kafkaProps;
    }

    protected Properties getDefaults() {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest"); // FIXME: switch to "latest" when ready
        return props;
    }

    public String getTopicName(String property) {
        return String.format("%s:%s", props.getProperty(STREAM_NAME), props.getProperty(property));
    }
}
