package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.StreamSupport;

import static java.lang.String.*;


/**
 * Consumes data from the general data stream and splits it into separate
 * streams, one for each car.
 * Streams consumer and producer at the same time.
 */
public class CarStreamsRouter {
    private KafkaConsumer<String, String> consumer;
    private String writeTopic;
    private KafkaProducer producer;

    public CarStreamsRouter(String confFilePath) {
        //CarStreamConsumer
        ConsumerConfigurer consumerConfig = new ConsumerConfigurer(confFilePath);
        String readTopic = consumerConfig.getReadTopic();
        consumer = new KafkaConsumer<>(consumerConfig.getKafkaProps());
        consumer.subscribe(Arrays.asList(readTopic));

        //Producer
        ProducerConfigurer producerConfig = new ProducerConfigurer(confFilePath);
        writeTopic = producerConfig.getTopic();
        producer = new KafkaProducer<>(producerConfig.getKafkaProps());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            producer.close();
        }));
    }

    public void start() {
        long pollTimeOut = 1000;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (records.isEmpty()) {
                System.out.println("No data arrived...");
            } else {
                Iterable<ConsumerRecord<String, String>> iterable = records::iterator;
                StreamSupport.stream(iterable.spliterator(), false).map(ConsumerRecord::value)
                        .forEach((recordValue) -> {
                            System.out.println("Consuming: " + recordValue);
                            decodeAndSend(recordValue);
                        });
            }
        }
    }

    private void decodeAndSend(String recordValue) {
        try {
            JSONObject record = new JSONObject(recordValue);
            long timestamp = record.getLong("timestamp");
            Double raceTime = record.getDouble("racetime");
            System.out.println(record);
            JSONArray carsInfo = record.getJSONArray("cars");
            for (int i = 0; i < carsInfo.length(); i++) {
                JSONObject carInfo = carsInfo.getJSONObject(i);
                carInfo.put("racetime", raceTime);
                carInfo.put("timestamp", timestamp);
                Integer carId = carInfo.getInt("id");
                carInfo.remove("id");
                ProducerRecord<Object, byte[]> rec = new ProducerRecord<>(format(writeTopic, carId), carInfo.toString().getBytes());
                producer.send(rec, (recordMetadata, e) -> {
                    if (e != null) {
                        System.out.println("Exception occurred while sending :(");
                        System.out.println(e.toString());
                        return;
                    }
                    System.out.println("Sent: " + recordMetadata.topic() + " # " + recordMetadata.partition() + " MSG: " + carInfo.toString());
                });
            }
        } catch (JSONException e) {
            System.out.println("Error during processing record " + recordValue);
            e.printStackTrace();
        }
    }

    private class ConsumerConfigurer extends Configurer {
        public ConsumerConfigurer(String pathToProps) {
            super(pathToProps);
        }

        public String getReadTopic() {
            return getTopicName(TOPIC_CARS_ALL);
        }
    }

    private static class ProducerConfigurer extends Configurer {
        public ProducerConfigurer(String pathToProps) {
            super(pathToProps);
        }

        public String getTopic() {
            return getTopicName(TOPIC_CARS_SINGLE);
        }
    }
}
