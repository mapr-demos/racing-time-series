package com.mapr.examples.telemetryagent.test;

import com.mapr.examples.telemetryagent.util.Batcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

import static org.apache.commons.lang.StringUtils.leftPad;


public class TestProducer {
    public static final String TOPIC = "/test_stream:topic1";
    private KafkaProducer<String, byte[]> producer;

    public TestProducer() {
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

        producer = new KafkaProducer<>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
        }));
    }

    private Batcher<JSONObject> b = new Batcher<>("prod", 5, (batch) -> {
        ProducerRecord<String, byte[]> rec;
        JSONArray array = new JSONArray(batch);
        rec = new ProducerRecord<>(TOPIC, array.toString().getBytes());
        producer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                System.err.println("Exception occurred while sending :(");
                System.err.println(e.toString());
                return;
            }
        });
    });

    public void start() {
        try {
            int i = 0;
            Random r = new Random();
            while (true) {
//                Thread.sleep(10);
                JSONObject obj = new JSONObject();
                obj.put("id", i);
                obj.put("rand", gen(r.nextInt(1000)));
//                b.add(obj);
                ProducerRecord<String, byte[]> rec = new ProducerRecord<>(TOPIC, obj.toString().getBytes());
                producer.send(rec, (recordMetadata, e) -> {
                    if (e != null) {
                        System.err.println("Exception occurred while sending :(");
                        System.err.println(e.toString());
                        return;
                    }
                });
                i ++;
            }
//        } catch (InterruptedException e) {
//            System.out.println(e);
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static String gen(int length) {
        StringBuffer sb = new StringBuffer();
        for (int i = length; i > 0; i -= 12) {
            int n = Math.min(12, Math.abs(i));
            sb.append(leftPad(Long.toString(Math.round(Math.random() * Math.pow(36, n)), 36), n, '0'));
        }
        return sb.toString();
    }
}
