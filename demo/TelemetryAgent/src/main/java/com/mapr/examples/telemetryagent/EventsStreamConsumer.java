package com.mapr.examples.telemetryagent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.StreamSupport;


/**
 * Consumer for the stream of events, writes events to the DB
 */
public class EventsStreamConsumer {
    public static final String RACE_STARTED = "raceStarted";

    private KafkaConsumer<String, String> consumer;
    private String topic;
    private ConsumerConfigurer configurer;
    private CarsDAO carsDAO;

    public EventsStreamConsumer(String confFilePath) {
        configurer = new ConsumerConfigurer(confFilePath);
        topic = configurer.getTopic();
        consumer = new KafkaConsumer<>(configurer.getKafkaProps());
        consumer.subscribe(Arrays.asList(topic));
        carsDAO = new CarsDAO();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
    }

    public void start() {
        long pollTimeOut = 100;
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
            if (!records.isEmpty()) {
                Iterable<ConsumerRecord<String, String>> iterable = records::iterator;
                StreamSupport.stream(iterable.spliterator(), false).forEach(this::processEvent);
                consumer.commitSync();
            }
        }
    }

    private void processEvent(ConsumerRecord<String, String> record) {
        switch (record.key()) {
            case RACE_STARTED:
                System.out.println(">> RACE_START event stored");
                carsDAO.newRace(record.value());
                break;
            default:
                System.err.println("Unknown event " + record.key());
        }
    }

    private class ConsumerConfigurer extends Configurer {
        public ConsumerConfigurer(String pathToProps) {
            super(pathToProps);
        }
        public String getTopic() {
            return getTopicName(TOPIC_EVENTS);
        }
    }
}