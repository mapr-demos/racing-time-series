package com.mapr.samples.racingseries.api;

import com.mapr.db.MapRDB;
import com.mapr.examples.telemetryagent.CarsDAO;
import com.mapr.examples.telemetryagent.LiveConsumer;
import com.mapr.examples.telemetryagent.beans.TelemetryRecord;
import com.mapr.samples.racingseries.dto.CarTelemetryDataPoint;
import com.mapr.samples.racingseries.dto.TelemetryTimestamp;
import com.mapr.samples.racingseries.dto.TelemetryTimestampsResponse;
import org.eclipse.jetty.servlets.EventSource;
import org.eclipse.jetty.servlets.EventSourceServlet;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RealTimeApi extends EventSourceServlet {
    private static final long serialVersionUID = 1L;

    private LiveConsumer consumer;
    private String CONFIG_PATH = "/vagrant/TelemetryAgent/src/main/resources/live_consumer.conf";

    public RealTimeApi() {
        consumer = new LiveConsumer(CONFIG_PATH);
    }

    @Override
    protected EventSource newEventSource(final HttpServletRequest req) {
        return new TimestampSource(consumer);
    }

    protected static class TimestampSource implements EventSource, LiveConsumer.Listener {
        private ConcurrentLinkedQueue<JSONObject> timestampsQueue = new ConcurrentLinkedQueue<>();
        private ConcurrentLinkedQueue<JSONObject> racesQueue = new ConcurrentLinkedQueue<>();
        private ConcurrentLinkedQueue<String> warmupQueue = new ConcurrentLinkedQueue<>();

        private LiveConsumer poller;
        private long actualTimestamp;

        public TimestampSource(LiveConsumer poller) {
            this.poller = poller;
        }

        @Override
        public void onOpen(final EventSource.Emitter emitter) throws IOException {
            poller.subscribe(this);
            emitter.event("test", "Event source opened");
            System.out.println("opened");
            while (true) {
                emitWarmup(emitter);
                emitRaces(emitter);
                emitTimestamps(emitter);
            }
        }

        private void emitWarmup(Emitter emitter) throws IOException {
            String value;
            do {
                value = warmupQueue.poll();
                if (value != null) {
                    emitter.event("test", value);
                }
            } while (value != null);
        }

        private void emitRaces(Emitter emitter) throws IOException {
            JSONObject race;
            do {
                race = racesQueue.poll();
                if (race != null) {
                    emitter.event("raceStarted", race.toString());
                }
            } while (race != null);
        }

        private void emitTimestamps(Emitter emitter) throws IOException {
            final int MAX_SIZE = 200;
            long t = System.currentTimeMillis();
            List<JSONObject> frozenQueue = new ArrayList<>(MAX_SIZE);
            do {
                JSONObject obj = timestampsQueue.poll();
                if (obj == null) break;
                frozenQueue.add(obj);
            } while (frozenQueue.size() < MAX_SIZE);

            Stream<TelemetryRecord> newRecords = frozenQueue.stream().
                    map(JSONObject::toString).
                    map(MapRDB::newDocument).
                    map(CarsDAO::documentToRecord);

            TelemetryTimestampsResponse response = new TelemetryTimestampsResponse();

            List<TelemetryTimestamp> telemetryTimestamps = newRecords.
                    map(telemetryRecord -> {
                        ArrayList<CarTelemetryDataPoint> singleCarTimestamp = new ArrayList<>();
                        singleCarTimestamp.add(
                                new CarTelemetryDataPoint(((Double)telemetryRecord.getCarId()).intValue(),
                                        telemetryRecord.getSensors())
                        );

                        return new TelemetryTimestamp(telemetryRecord.getRacetime(), singleCarTimestamp);
                    }).
                    collect(Collectors.toList());

            if (!telemetryTimestamps.isEmpty()) {
                response.setTimestamps(telemetryTimestamps);
                System.out.println("1: " + t + " -> " + System.currentTimeMillis());

                emitter.data(new JSONObject(response).toString());
            }
        }

        @Override
        public void onClose() {
            poller.unsubscribe(this);
            System.out.println("closed");
        }

        @Override
        public void onNewData(JSONObject data) {
            if (actualTimestamp != 0 && data.getLong("timestamp") == actualTimestamp) {
                timestampsQueue.add(data);
            }
        }

        @Override
        public void onRaceStarted(JSONObject value) {
            racesQueue.add(value);
            actualTimestamp = value.getLong("timestamp");
        }

        @Override
        public void onTestMessage(String value) {
            warmupQueue.add(value);
        }
    }
}
