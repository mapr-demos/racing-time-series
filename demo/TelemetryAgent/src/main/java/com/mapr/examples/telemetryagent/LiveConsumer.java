package com.mapr.examples.telemetryagent;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class LiveConsumer implements Runnable {
    private KafkaConsumer<String, String> consumer;
    private ConsumerConfigurer configurer;

    private String eventsTopic;
    private Map<String, Integer> topicsForCars = new HashMap<>();

    public LiveConsumer(String confFilePath) {
        configurer = new ConsumerConfigurer(confFilePath);
        eventsTopic = getEventsTopicName();

        consumer = new KafkaConsumer<>(configurer.getKafkaProps());
        List<String> topics = getCarTopics();
        topics.add(eventsTopic);

        consumer.subscribe(topics);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
    }

    private List<String> getCarTopics() {
        List<String> carTopics = new ArrayList<>();
        for (int i=1;i<11;i++){
            String topicName = String.format(configurer.getTopic(), i);
            carTopics.add(topicName);
            topicsForCars.put(topicName, i);
        }
        return carTopics;
    }

    private String getEventsTopicName() {
        return configurer.getTopicName(Configurer.TOPIC_EVENTS);
    }

    private double old;
    @Override
    public void run() {
        long pollTimeOut = 0;

        ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
        if (!records.isEmpty()) {
            System.out.println("New items 1: " + records.count());
            consumer.commitAsync();
            for(ConsumerRecord<String, String> record : records) {
                try {

                    if (record.topic().equals(eventsTopic)) {
                        eventReceived(record);
                    } else {
                        JSONArray array = new JSONArray(record.value());
                        for (int i = 0; i < array.length(); i++) {
                            JSONObject recordJSON = (JSONObject) array.get(i);
                            recordJSON.put("carId", topicsForCars.get(record.topic()));
                            if (topicsForCars.get(record.topic()) == 8) {
                                if (recordJSON.getDouble("racetime") < old) {
                                    System.out.println(">> EXTERMINATE " + recordJSON.getDouble("racetime") + " < " + old);
                                }
                                old = recordJSON.getDouble("racetime");
                            }
                            onNewData(recordJSON);
                        }
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void eventReceived(ConsumerRecord<String, String> record) throws JSONException {
        switch (record.key()) {
            case EventsStreamConsumer.RACE_STARTED:
                System.out.println(">> RACE_START event found");
                onRaceStarted(new JSONObject(record.value()));
                break;
            default:
                System.err.println("Unknown event " + record.key());
        }
    }

    private Set<Listener> listeners = Collections.synchronizedSet(new HashSet<>());

    public void onNewData(JSONObject arg) {
        listeners.forEach((l) -> l.onNewData(arg));
    }

    public void onRaceStarted(JSONObject value) {
        listeners.forEach((l) -> l.onRaceStarted(value));
    }

    public void subscribe(Listener l) {
        listeners.add(l);
    }

    public void unsubscribe(Listener l) {
        listeners.remove(l);
    }

    public interface Listener {
        void onNewData(JSONObject data);
        void onRaceStarted(JSONObject value);
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