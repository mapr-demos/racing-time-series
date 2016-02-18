package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.*;

import java.util.*;
import java.util.stream.StreamSupport;


/**
 * Handler of one car. Receives data from the stream of the car
 * (id passed to the constructor) and saves data to the MapR JSON DB.
 * Each record consists of the timestamp since the race start and
 * sensors data.
 */
public class CarStreamConsumer {

    private KafkaConsumer<String, String> consumer;
    private String topic;
    private ConsumerConfigurer configurer;
    private CarsDAO carsDAO;

    public CarStreamConsumer(String confFilePath, int id) {
        configurer = new ConsumerConfigurer(confFilePath);
        topic = String.format(configurer.getTopic(), id);
        System.out.println(topic);
        consumer = configureConsumer();
        consumer.subscribe(Arrays.asList(topic));
        carsDAO = new CarsDAO(topic);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
    }

    public void start() {
        long pollTimeOut = 1000;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (records.isEmpty()) {
                System.out.println("No data arrived...");
            } else {
                Iterable<ConsumerRecord<String, String>> iterable = records::iterator;
                StreamSupport.stream(iterable.spliterator(), false).forEach((record) -> {
                    System.out.println("Consuming: " + record.toString() + " from " + this.topic);
                    carsDAO.insert(record.value());
                });
            }
        }
    }

    private KafkaConsumer<String, String> configureConsumer() {
        for(Map.Entry entry: configurer.getKafkaProps().entrySet()) {
            System.out.println(String.format(entry.getKey().toString(), entry.getValue().toString()));
        }
        return new KafkaConsumer<>(configurer.getKafkaProps());
    }


    private class ConsumerConfigurer extends Configurer {
        public ConsumerConfigurer(String pathToProps) {
            super(pathToProps);
        }
        public String getTopic() {
            return getTopicName(TOPIC_CARS_SINGLE);
        }
    }
}