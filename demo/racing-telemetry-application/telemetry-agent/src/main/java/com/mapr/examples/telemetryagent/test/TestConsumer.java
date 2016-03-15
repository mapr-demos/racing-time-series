package com.mapr.examples.telemetryagent.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;


public class TestConsumer {

    private KafkaConsumer<String, String> consumer;

    public TestConsumer() {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "TEST");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TestProducer.TOPIC));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
    }

    public void start() {
        long pollTimeOut = 10;
        int index = 0;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JSONArray ar = new JSONArray(record.value());
                        for (int i=0; i<ar.length(); i++) {
                            JSONObject item = (JSONObject) ar.get(i);
                            int newIndex = item.getInt("id");
                            if (newIndex < index) {
                                System.out.println("ERROR INDEX " + newIndex + " < " + index);
                            }
                            index = newIndex;
                            System.out.println(index);
                        }
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
                consumer.commitAsync();
            }
        }
    }
}