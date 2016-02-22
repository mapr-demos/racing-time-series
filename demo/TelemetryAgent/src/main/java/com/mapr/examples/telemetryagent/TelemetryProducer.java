package com.mapr.examples.telemetryagent;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Producer of the telemetry records from TORCS to stream.
 * Continuously read tail of log file similar to UNIX `tail --follow` utility,
 * formats data to JSON and sends records to the stream.
 * Each record consists of the timestamp and objects with telemetry of each car.
 */
public class TelemetryProducer {

    private String topic;

    private ProducerConfigurer configurer;
    private KafkaProducer producer;

    private File logFile = null;
    private long readTimeout = 50*1000;
    private long lastKnownPosition = 0;
    private long readLineCounter = 0;

    private LogsToJSONConverter logsToJSONConverter;

    public TelemetryProducer(String pathToProps, String logFilePath, int readTimeout) {
        configurer = new ProducerConfigurer(pathToProps);
        topic = configurer.getTopic();
        producer = new KafkaProducer<>(configurer.getKafkaProps());
        //TODO: set cursor
        logFile = new File(logFilePath);
        this.readTimeout = readTimeout;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
        }));
    }

    public void start() {

        try {
            // Continuously read tail of log file and send to the stream
            while (true) {
                Thread.sleep(readTimeout);

                long fileLength = logFile.length();
                if (fileLength > lastKnownPosition) {

                    // Reading and writing file
                    RandomAccessFile readWriteFileAccess = new RandomAccessFile(logFile, "r");
                    readWriteFileAccess.seek(lastKnownPosition);
                    String logLine;

                    while ((logLine = readWriteFileAccess.readLine()) != null) {
                        long timestamp = new Date().getTime() / 1000;
                        if (readLineCounter == 0) {  // Reading headers
                            this.logsToJSONConverter = new LogsToJSONConverter(logLine, timestamp);
                            eventRaceStarted(timestamp);
                        } else { // Reading telemetry line
                            sendTelemetryToStream(logLine);
                        }
                        readLineCounter++;
                    }
                    lastKnownPosition = readWriteFileAccess.getFilePointer();
                    readWriteFileAccess.close();
                } else if (fileLength == lastKnownPosition) {
                    System.out.println("Hmm.. Couldn't found new line after line # " + readLineCounter);
                } else {
                    // File was overwritten
                    lastKnownPosition = 0;
                    readLineCounter = 0;
                }
            }
        } catch (InterruptedException | IOException e) {
            System.out.println(e);
        } finally {
            producer.close();
        }
    }

    private void eventRaceStarted(long timestamp) {
        JSONObject event = new JSONObject();
        try {
            event.put("timestamp", timestamp);
            event.put("carsCount", logsToJSONConverter.getCarsCount());
            event.put("carIds", logsToJSONConverter.getCarNumbers());
        } catch (JSONException e) {
            e.printStackTrace();
        }
        sendEvent(EventsStreamConsumer.RACE_STARTED, event);
    }

    private void sendEvent(String eventName, JSONObject value) {
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(
                configurer.getTopicName(Configurer.TOPIC_EVENTS),
                eventName,
                value.toString().getBytes());
        producer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                System.err.println("Exception occurred while sending :(");
                System.err.println(e.toString());
                return;
            }
            System.out.println("Sent: " + recordMetadata.topic() + " # " + recordMetadata.partition() +
                    " MSG: " + value);
        });
    }

    private void sendTelemetryToStream(String logLine) {
        ProducerRecord<Void, byte[]> rec;
        JSONObject jsonRecord;
        try {
            jsonRecord = this.logsToJSONConverter.formatJSONRecord(logLine);
            rec = new ProducerRecord<>(topic, jsonRecord.toString().getBytes());
            producer.send(rec, (recordMetadata, e) -> {
                if (e != null) {
                    System.err.println("Exception occurred while sending :(");
                    System.err.println(e.toString());
                    return;
                }
                System.out.println("Sent: " + recordMetadata.topic() + " # " + recordMetadata.partition() +
                        " MSG: " + jsonRecord.toString());
            });
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    protected static class LogsToJSONConverter {
        Set<String> headers;

        Set<Integer> carNumbers;

        long timestamp;
        LogsToJSONConverter(String headersLine, long timestamp) {
            this.timestamp = timestamp;
            this.headers = new LinkedHashSet<>();
            this.carNumbers = new LinkedHashSet<>();
            Matcher m = Pattern.compile("(\\w*)(\\d+)")
                    .matcher(headersLine);
            while (m.find()) {
                headers.add(m.group(1));
                carNumbers.add(Integer.valueOf(m.group(2)));
            }

            System.out.println("Data formatter create");
        }

        public JSONObject formatJSONRecord(String line) throws JSONException {
            String[] splittedLine =  line.split(" ");
            Double time = Double.parseDouble(splittedLine[0]);

            JSONObject jsonTelemetryRecord = new JSONObject();
            JSONArray allCarsJSON = new JSONArray();

            List<String> sensorNames = Arrays.asList(Arrays.copyOfRange(splittedLine, 1, splittedLine.length));
            int sensorsPerCar = getSensorsCount(splittedLine);

            Iterator<List<String>> carsDataIt = Lists.partition(sensorNames, sensorsPerCar).iterator();
            Iterator<Integer> carNumberIt = carNumbers.iterator();
            while (carsDataIt.hasNext() && carNumberIt.hasNext()) {
                Iterator<String> carDataIt = carsDataIt.next().iterator();
                Iterator<String> headersIt = headers.iterator();

                JSONObject carJson = new JSONObject();
                while (carDataIt.hasNext() && headersIt.hasNext()) {
                    carJson.put(headersIt.next(), Double.parseDouble(carDataIt.next()));
                }

                JSONObject carOuterJson = new JSONObject();
                carOuterJson.put("id", carNumberIt.next());
                carOuterJson.put("sensors", carJson);

                allCarsJSON.put(carOuterJson);
            }
            jsonTelemetryRecord.put("racetime", time);
            jsonTelemetryRecord.put("timestamp", timestamp);
            jsonTelemetryRecord.put("cars", allCarsJSON);
            return jsonTelemetryRecord;
        }

        private int getSensorsCount(String[] colNames) {
            return (colNames.length-1)/ getCarsCount();
        }

        public int getCarsCount() {
            return carNumbers.size();
        }

        public Set<Integer> getCarNumbers() {
            return carNumbers;
        }
    }

    private static class ProducerConfigurer extends Configurer {
        public ProducerConfigurer(String pathToProps) {
            super(pathToProps);
        }

        public String getTopic() {
            return getTopicName(TOPIC_CARS_ALL);
        }
    }
}
