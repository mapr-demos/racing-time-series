package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
//        System.out.println(topic);
        consumer = new KafkaConsumer<>(configurer.getKafkaProps());
        consumer.subscribe(Arrays.asList(topic));
        carsDAO = new CarsDAO(id, String.format("car%d", id));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
    }

    public void start() {
        long pollTimeOut = 1000;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (records.isEmpty()) {
//                System.out.println("No data arrived...");
            } else {
                Iterable<ConsumerRecord<String, String>> iterable = records::iterator;
                StreamSupport.stream(iterable.spliterator(), false).forEach((record) -> {
//                    System.out.println("Consuming: " + record.toString() + " from " + this.topic);
                    try {
                        JSONArray array = new JSONArray(record.value());
                        for (int i = 0; i < array.length(); i++) {
                            carsDAO.insert(array.get(i).toString());
                        }
                    } catch (JSONException e) {
                        System.out.println("Error processing records " + record.value());
                        e.printStackTrace();
                    }
                });
                consumer.commitAsync();
            }
        }
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