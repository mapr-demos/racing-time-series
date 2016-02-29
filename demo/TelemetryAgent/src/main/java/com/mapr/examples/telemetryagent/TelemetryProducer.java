package com.mapr.examples.telemetryagent;

import com.google.common.collect.Lists;
import com.mapr.examples.telemetryagent.beans.Race;
import com.mapr.examples.telemetryagent.util.Batcher;
import org.apache.kafka.clients.producer.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
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

    public static final String TMP_TELEMETRY_POINTER_FILE = "/tmp/telemetryPointer";
    private String topic;

    private ProducerConfigurer configurer;
    private KafkaProducer<String, byte[]> producer;

    private File logFile = null;
    private long readTimeout = 50*1000;
    private long lastKnownPosition = 0;
    private long readLineCounter = 0;

    private LogsToJSONConverter logsToJSONConverter;

    public TelemetryProducer(String pathToProps, String logFilePath, int readTimeout) {
        configurer = new ProducerConfigurer(pathToProps);
        topic = configurer.getTopic();
        producer = new KafkaProducer<>(configurer.getKafkaProps());
        logFile = new File(logFilePath);
        this.readTimeout = readTimeout;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
        }));
    }

    public void start() {

        try {
            // Continuously read tail of log file and send to the stream
            long fileLength = logFile.length();
            lastKnownPosition = loadPointer(fileLength);
            while (true) {
                Thread.sleep(readTimeout);

                fileLength = logFile.length();
                if (logFile.exists() && fileLength > lastKnownPosition) {

                    // Reading and writing file
                    RandomAccessFile readFileAccess = new RandomAccessFile(logFile, "r");
                    readFileAccess.seek(lastKnownPosition);
                    String logLine;

                    while ((logLine = readFileAccess.readLine()) != null) {
                        long timestamp = new Date().getTime() / 1000;
                        if (readLineCounter == 0) {  // Reading headers
                            this.logsToJSONConverter = new LogsToJSONConverter(logLine, timestamp);
                            eventRaceStarted(timestamp);
                        } else { // Reading telemetry line
//                            System.out.println("!! " + readFileAccess.getFilePointer() + "; " + fileLength);
                            if (readFileAccess.getFilePointer() < fileLength - 1) {
                                sendTelemetryToStream(logLine);
                            } else {
                                // Last line read
                                lastKnownPosition = readFileAccess.getFilePointer() - logLine.length();
                                break;
                            }
                        }
                        readLineCounter++;
                    }
//                    lastKnownPosition = readFileAccess.getFilePointer();
                    readFileAccess.close();
                    persistPointer(fileLength, lastKnownPosition, readLineCounter);
                } else if (lastKnownPosition > 0 && fileLength == lastKnownPosition) {
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

    private void persistPointer(long fileSize, long lastKnownPosition, long readLineCounter) {
        try {
            PrintWriter writer = new PrintWriter(TMP_TELEMETRY_POINTER_FILE, "UTF-8");
            writer.println(fileSize);
            writer.println(lastKnownPosition);
            writer.println(readLineCounter);
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            System.err.println("Error during persisting the telemetry pointer");
            e.printStackTrace();
        }
    }

    private long loadPointer(long fileSize) {
        long pointer = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(TMP_TELEMETRY_POINTER_FILE));
            long originalFileSize = Long.parseLong(reader.readLine());
            if (originalFileSize == fileSize) {
                pointer = Long.parseLong(reader.readLine());
                readLineCounter = Long.parseLong(reader.readLine());
            }
            reader.close();
        } catch (IOException e) {
            System.err.println("Telemetry log was changed, reading from beginning");
        }
        return pointer;
    }

    private void eventRaceStarted(long timestamp) {
        Race race = new Race();
        race.setTimestamp(timestamp);
        race.setCarsCount(logsToJSONConverter.getCarsCount());

        JSONObject raceJson = new JSONObject(race);
        race.setCarIds(new ArrayList<>());

        JSONArray carIds = new JSONArray();
        logsToJSONConverter.getCarNumbers().forEach(carIds::put);
        try {
            raceJson.put("carIds", carIds);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        sendEvent(EventsStreamConsumer.RACE_STARTED, raceJson);
    }

    private void sendEvent(String eventName, JSONObject value) {
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(
                configurer.getTopicName(Configurer.TOPIC_EVENTS),
                eventName,
                value.toString().getBytes());
        System.out.println(">> Event " + eventName + ": " + value.toString());
        producer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                System.err.println("Exception occurred while sending :(");
                System.err.println(e.toString());
                return;
            }
        });
    }

    private void sendTelemetryToStream(String logLine) {
        JSONObject jsonRecord;
        try {
            jsonRecord = this.logsToJSONConverter.formatJSONRecord(logLine);
            telemetryBatch.add(jsonRecord);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    private Batcher<JSONObject> telemetryBatch = new Batcher<>("prod", 50, (batch) -> {
        ProducerRecord<String, byte[]> rec;
        JSONArray array = new JSONArray(batch);
        rec = new ProducerRecord<>(topic, array.toString().getBytes());
        producer.send(rec, (recordMetadata, e) -> {
            if (e != null) {
                System.err.println("Exception occurred while sending :(");
                System.err.println(e.toString());
                return;
            }
        });
    });

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
