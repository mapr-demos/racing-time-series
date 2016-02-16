package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

import static java.lang.String.*;


public class CarStreamsRouter {
    public final KafkaConsumer<String, String> consumer;
    private String writeTopic;
    private KafkaProducer producer;

    public CarStreamsRouter(String confFilePath) {
        //Consumer
        ConsumerConfigurer consumerConfig = new ConsumerConfigurer(confFilePath);
        String readTopic = consumerConfig.getReadTopic();
        consumer = new KafkaConsumer<>(consumerConfig.getKafkaProps());
        consumer.subscribe(Arrays.asList(readTopic));

        //Producer
        ProducerConfigurer producerConfig = new ProducerConfigurer(confFilePath);
        writeTopic = producerConfig.getTopic();
        producer = new KafkaProducer<>(producerConfig.getKafkaProps());
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.close();
                producer.close();
            }
        }));
    }

    public void start() {
        long pollTimeOut = 1000;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);

            Iterator<ConsumerRecord<String, String>> iter = records.iterator();
            if (!iter.hasNext()) {
                System.out.println("Empty :(");
            }
            while(iter.hasNext()) {
                ConsumerRecord<String, String> record = iter.next();
                String recordValue = record.value();
                System.out.println("Consuming: " + recordValue);
                decodeAndSend(recordValue);
            }
        }
    }

    private void decodeAndSend(String recordValue) {
        try {
            JSONObject record = new JSONObject(recordValue);
            Double timeStamp = record.getDouble("time");
            System.out.println(record);
            JSONArray carsInfo = record.getJSONArray("cars");
            for (int i = 0; i < carsInfo.length(); i++) {
                final JSONObject carInfo = carsInfo.getJSONObject(i);
                carInfo.put("time", timeStamp);
                Integer carId = carInfo.getInt("id");
                ProducerRecord<Object, byte[]> rec = new ProducerRecord<>(format(writeTopic, carId), carInfo.toString().getBytes());
                producer.send(rec, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            System.out.println("Exception occurred while sending :(");
                            System.out.println(e.toString());
                            return;
                        }
                        System.out.println("Sent: " + recordMetadata.topic() + " # " + recordMetadata.partition() + " MSG: " + carInfo.toString());
                    }
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
