package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.*;

import java.util.*;



public class Consumer {

    public String topic;

    public final KafkaConsumer<String, String> consumer;

    private ConsumerConfigurer configurer;

    public Consumer(String confFilePath) {
        configurer = new ConsumerConfigurer(confFilePath);
        topic = configurer.getTopic();
        consumer = configureConsumer();
        consumer.subscribe(Arrays.asList(topic));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.close();
            }
        }));
    }

    public void start() {
        long pollTimeOut = 1000;
        while(true) {
            System.out.println(consumer.listTopics());
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);

            Iterator<ConsumerRecord<String, String>> iter = records.iterator();
            if (!iter.hasNext()) {
                System.out.println("Empty :(");
            }
            while(iter.hasNext()) {
                ConsumerRecord<String, String> record = iter.next();
                System.out.println("Consuming: " + record.toString());
            }
        }
    }

    public KafkaConsumer<String, String> configureConsumer() {
        for(Map.Entry entry: configurer.getKafkaProps().entrySet()) {
            System.out.println(String.format(entry.getKey().toString(), entry.getValue().toString()));
        }
        return new KafkaConsumer<>(configurer.getKafkaProps());
    }


    private class ConsumerConfigurer extends Configurer {
        public ConsumerConfigurer(String pathToProps) {
            super(pathToProps);
        }
        @Override
        protected Properties getDefaults() {
            Properties props = new Properties();
            props.put("key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("enable.auto.commit", "true");
            return props;
        }
        public String getTopic() {
            return getTopicName(TOPIC_CARS_ALL);
        }
    }
}