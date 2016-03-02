package com.mapr.examples.telemetryagent;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LiveConsumer {
    private ConsumerConfigurer configurer;

    private String eventsTopic;
    private List<String> carTopics;
    private Map<String, Integer> topicsForCars = new HashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(11);
    private List<BaseConsumer> consumers = new ArrayList<>();

    public LiveConsumer(String confFilePath) {
        configurer = new ConsumerConfigurer(confFilePath);
        eventsTopic = getEventsTopicName();
        carTopics = getCarTopics();

        consumers.add(new EventsConsumer(eventsTopic));
        for (String topic : carTopics) {
            consumers.add(new SingleCarConsumer(topic, topicsForCars.get(topic)));
        }
    }

    public abstract class BaseConsumer implements Runnable {
        private KafkaConsumer<String, String> consumer;

        public BaseConsumer(String topic) {
            consumer = new KafkaConsumer<>(configurer.getKafkaProps());
            consumer.subscribe(Arrays.asList(topic));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumer.close();
            }));

            scheduler.scheduleWithFixedDelay(this, 500, 500, TimeUnit.MILLISECONDS);
        }

        @Override
        public void run() {
            long pollTimeOut = 450;

            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (!records.isEmpty()) {
                processRecords(records);
                consumer.commitAsync();
            }
        }

        protected abstract void processRecords(ConsumerRecords<String, String> records);
    }

    public class SingleCarConsumer extends BaseConsumer {
        private Integer id;

        public SingleCarConsumer(String topic, Integer id) {
            super(topic);
            this.id = id;
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            System.out.println("New items on " + id + ": " + records.count());
            for(ConsumerRecord<String, String> record : records) {
                try {
                    JSONArray array = new JSONArray(record.value());
                    for (int i = 0; i < array.length(); i++) {
                        JSONObject recordJSON = (JSONObject) array.get(i);
                        recordJSON.put("carId", id);
                        onNewData(recordJSON);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class EventsConsumer extends BaseConsumer {
        public EventsConsumer(String topic) {
            super(topic);
        }

        protected void processRecords(ConsumerRecords<String, String> records) {
            System.out.println("New items on events: " + records.count());
            for(ConsumerRecord<String, String> record : records) {
                try {
                    switch (record.key()) {
                        case EventsStreamConsumer.RACE_STARTED:
                            System.out.println(">> RACE_START event found");
                            onRaceStarted(new JSONObject(record.value()));
                            break;
                        default:
                            System.err.println("Unknown event " + record.key());
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
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

//    @Override
//    public void run() {
//        long pollTimeOut = 0;
//
//        ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
//        if (!records.isEmpty()) {
//            System.out.println("New items: " + records.count());
//            consumer.commitAsync();
//            for(ConsumerRecord<String, String> record : records) {
//                try {
//                    if (record.topic().equals(eventsTopic)) {
//                        eventReceived(record);
//                    } else {
//                        JSONArray array = new JSONArray(record.value());
//                        for (int i = 0; i < array.length(); i++) {
//                            JSONObject recordJSON = (JSONObject) array.get(i);
//                            recordJSON.put("carId", topicsForCars.get(record.topic()));
//                            onNewData(recordJSON);
//                        }
//                    }
//
//                } catch (JSONException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

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