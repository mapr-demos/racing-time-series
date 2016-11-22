package com.mapr.examples.telemetryagent;

import com.mapr.examples.telemetryagent.util.Batcher;
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
    private KafkaProducer<String, byte[]> producer;

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
        long pollTimeOut = 10;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (!records.isEmpty()) {
                Iterable<ConsumerRecord<String, String>> iterable = records::iterator;
                StreamSupport.stream(iterable.spliterator(), false)
                        .forEach(this::decodeAndSend);
                consumer.commitAsync();
            }
        }
    }

    private void decodeAndSend(ConsumerRecord<String, String> record) {
        String recordValue = record.value();
        JSONArray records;
        try {
            records = new JSONArray(recordValue);
            for (int i = 0; i < records.length(); i++) {
                decodeSingleRecordAndSend((JSONObject) records.get(i));
            }
        } catch (JSONException e) {
            System.err.println("Error during processing records " + recordValue);
            e.printStackTrace();
        }
    }

    /**
     * Key is the car topic
     */
    private Map<String, Batcher<JSONObject>> telemetryBatchers = new HashMap<>();
    private Batcher<JSONObject> getBatcher(String topic) {
        if (!telemetryBatchers.containsKey(topic)) {

            Batcher<JSONObject> batcher = new Batcher<>(topic, 5, (batch) -> {
                JSONArray array = new JSONArray(batch);
                ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, array.toString().getBytes());
                producer.send(rec, (recordMetadata, e) -> {
                    if (e != null) {
                        System.out.println("Exception occurred while sending :(");
                        System.out.println(e.toString());
                        return;
                    }
                });
                producer.flush();
            });

            telemetryBatchers.put(topic, batcher);
        }
        return telemetryBatchers.get(topic);
    }

    private void decodeSingleRecordAndSend(JSONObject record) {
        try {
            long timestamp = record.getLong("timestamp");
            double raceTime = record.getDouble("racetime");
            String raceId = record.getString("race_id");

            JSONArray carsInfo = record.getJSONArray("cars");
            for (int i = 0; i < carsInfo.length(); i++) {
                JSONObject carInfo = carsInfo.getJSONObject(i);
                carInfo.put("racetime", raceTime);
                carInfo.put("timestamp", timestamp);
                carInfo.put("race_id", raceId);
                Integer carId = carInfo.getInt("id");

                getBatcher(format(writeTopic, carId)).add(carInfo);
                postSensorData(carInfo);
            }
        } catch (JSONException e) {
            System.out.println("Error during processing record " + record.toString());
            e.printStackTrace();
        }
    }

  /**
   * Post single sensor data to the sensor_data topic
   * @param json
   */
  private void postSensorData(JSONObject json) {
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>("/apps/racing/stream:sensors_data", json.toString().getBytes());
        producer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                System.out.println("Exception occurred while sending :(");
                System.out.println(e.toString());
                return;
            }
        });


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
