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

    private KafkaProducer producer;

    private File logFile = null;
    private long readTimeout = 50*1000;
    private long lastKnownPosition = 0;
    private static int readLineCounter = 0;

    private DataFormatter dataFormatter;

    public TelemetryProducer(String pathToProps, String logFilePath, int readTimeout) {
        ProducerConfigurer configurer = new ProducerConfigurer(pathToProps);
        topic = configurer.getTopic();
        producer = new KafkaProducer<>(configurer.getKafkaProps());
        logFile = new File(logFilePath);
        this.readTimeout = readTimeout;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                producer.close();
            }
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
                        if (readLineCounter == 0) {  // Reading headers
                            this.dataFormatter = new DataFormatter(logLine);
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

    private void sendTelemetryToStream(String logLine) {
        ProducerRecord<Double, byte[]> rec;
        final JSONObject jsonRecord;
        try {
            jsonRecord = this.dataFormatter.formatJSONRecord(logLine);
            System.out.println(jsonRecord.toString());
            rec = new ProducerRecord<>(topic, jsonRecord.toString().getBytes());
            producer.send(rec, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("Exception occurred while sending :(");
                        System.out.println(e.toString());
                        return;
                    }
                    System.out.println("Sent: " + recordMetadata.topic() + " # " + recordMetadata.partition() + " MSG: " + jsonRecord.toString());
                }
            });
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    private static class DataFormatter {
        List<String> headers;
        List<Integer> carNumbers;

        DataFormatter(String headersLine) {
            this.headers = new ArrayList<>();
            this.carNumbers = new ArrayList<>();
            Matcher m = Pattern.compile("(\\w*)(\\d+)")
                    .matcher(headersLine);
            while (m.find()) {
                addToList(headers, m.group(1));
                addToList(carNumbers, m.group(2));
            }

            System.out.println("Data formatter create");
        }

        private void addToList(List list, String value) {
            if (!list.contains(value)) {
                list.add(value);
            }
        }

        public JSONObject formatJSONRecord(String line) throws JSONException {
            String[] splittedLine =  line.split(" ");
            Double time = Double.parseDouble(splittedLine[0]);

            JSONObject jsonTelemetryRecord = new JSONObject();
            JSONArray allCarsJSON = new JSONArray();

            Iterator<List<String>> carsDataIt = Lists.partition(Arrays.asList(Arrays.copyOfRange(splittedLine, 2, splittedLine.length-1)), (splittedLine.length-1)/carNumbers.size()).iterator();
            Iterator<Integer> carNumberIt = carNumbers.iterator();
            while (carsDataIt.hasNext() && carNumberIt.hasNext()) {
                Iterator<String> carDataIt = carsDataIt.next().iterator();
                Iterator<String> headersIt = headers.iterator();

                JSONObject carJson = new JSONObject();
                while (carDataIt.hasNext() && headersIt.hasNext()) {
                    carJson.put(headersIt.next(), Double.parseDouble(carDataIt.next()));
                }

                JSONObject carOuterJson = new JSONObject();
                carOuterJson.put("id", String.valueOf(carNumberIt.next()));
                carOuterJson.put("sensors", carJson);

                allCarsJSON.put(carOuterJson);
            }
            jsonTelemetryRecord.put("time", time);
            jsonTelemetryRecord.put("cars", allCarsJSON);
            return jsonTelemetryRecord;
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
