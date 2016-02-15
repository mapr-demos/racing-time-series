package com.mapr.examples.telemetryagent;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Utility for loading properties
 */
public abstract class Configurer {
  public static final String TOPIC_CARS_ALL = "topic.cars.all";
  public static final String TOPIC_CARS_SINGLE = "topic.cars.single";

  protected Properties props;
  public static final String KAFKA_PROPERTY_PREFIX = "kafka";

  public Configurer(String pathToProps) {
    this.props = new Properties();
    try {
      props.load(new FileInputStream(pathToProps));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Properties getKafkaProps() {
    Properties kafkaProps = new Properties();
    kafkaProps.putAll(getDefaults());
    for (Map.Entry kafkaProperty: props.entrySet()) {
      if (((String)kafkaProperty.getKey()).startsWith(KAFKA_PROPERTY_PREFIX)) {
        kafkaProps.put(((String) kafkaProperty.getKey())
                .substring(KAFKA_PROPERTY_PREFIX.length() + 1), kafkaProperty.getValue());
      }
    }

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
    return props;
  }
}
