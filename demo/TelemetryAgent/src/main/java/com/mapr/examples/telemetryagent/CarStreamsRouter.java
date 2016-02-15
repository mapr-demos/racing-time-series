package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;


public class CarStreamsRouter {
    public final KafkaConsumer<String, String> consumer;
    private ConsumerConfigurer consumerConfig;

    private KafkaProducer producer;
    private ProducerConfigurer producerConfig;

    public CarStreamsRouter(String confFilePath) {
        //Consumer
        consumerConfig = new ConsumerConfigurer(confFilePath);
        String readTopic = consumerConfig.getReadTopic();
        consumer = new KafkaConsumer<>(consumerConfig.getKafkaProps());
        consumer.subscribe(Arrays.asList(readTopic));

        //Producer
        producerConfig = new ProducerConfigurer(confFilePath);
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
                decode(recordValue);
                System.out.println("Consuming: " + recordValue);
            }
        }
    }

    private void decode(String recordValue) {
        try {
            JSONObject record = new JSONObject(recordValue);
            System.out.println(record);

//            rec = new ProducerRecord<>(topic, jsonRecord.toString().getBytes());
//            producer.send(rec, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e != null) {
//                        System.out.println("Exception occurred while sending :(");
//                        System.out.println(e.toString());
//                        return;
//                    }
//                    System.out.println("Sent: " + recordMetadata.topic() + " # " + recordMetadata.partition() + " MSG: " + jsonRecord.toString());
//                }
//            });
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
            return (String) props.get(TOPIC_CARS_ALL);
        }
    }

    private static class ProducerConfigurer extends Configurer {
        public ProducerConfigurer(String pathToProps) {
            super(pathToProps);
        }

        public String getTopic() {
            return props.getProperty(TOPIC_CARS_ALL);
        }
    }
}
