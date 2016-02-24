package com.mapr.examples.telemetryagent;


import com.google.common.collect.Lists;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.examples.telemetryagent.beans.TelemetryData;
import com.mapr.examples.telemetryagent.beans.TelemetryRecord;
import com.mapr.examples.telemetryagent.beans.Race;
import com.mapr.examples.telemetryagent.util.NoRacesException;
import org.apache.commons.beanutils.BeanUtils;
import org.ojai.Document;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.DocumentNotFoundException;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CarsDAO {
    Table telemetryTable;
    Table racesTable;

    final static public String APPS_DIR = "/apps/telemetry/";
    public static final String RACES_TABLE = APPS_DIR + "races";
    private int id;

    public CarsDAO() {
        this.racesTable = this.getTable(RACES_TABLE);
    }

    public CarsDAO(int id, String carName) {
        this.id = id;
        this.telemetryTable = this.getTable(APPS_DIR + carName);
        this.racesTable = this.getTable(RACES_TABLE);
    }

    private synchronized int autoincRacesCounter() {
        final String RACES_SAVED_FIELD = "racesSaved";
        final String COUNTER_DOCUMENT_ID = "count";

        Document racesCount;
        try {
            racesCount = this.racesTable.findById(COUNTER_DOCUMENT_ID);
            if (racesCount == null) {
                racesCount = MapRDB.newDocument();
                racesCount.set(RACES_SAVED_FIELD, 0);
            }
        } catch (DocumentNotFoundException e) {
            racesCount = MapRDB.newDocument();
            racesCount.set(RACES_SAVED_FIELD, 0);
        }

        int counter = racesCount.getInt(RACES_SAVED_FIELD) + 1;
        racesCount.set(RACES_SAVED_FIELD, counter);

        this.racesTable.insertOrReplace(COUNTER_DOCUMENT_ID, racesCount);
        return counter;
    }

    private Table getTable(String tableName) {
        Table table;

        if (!MapRDB.tableExists(tableName)) {
            table = MapRDB.createTable(tableName); // Create the table if not already present
        } else {
            table = MapRDB.getTable(tableName); // get the table
        }
        return table;
    }

    public void newRace(String raceDataJson) {
        Document document = MapRDB.newDocument(raceDataJson).
                set("_type", "race");
        racesTable.insert(String.valueOf(autoincRacesCounter()), document);

        racesTable.flush();
    }

    public void insert(String recordValueJson) {
        if (telemetryTable == null) {
            throw new RuntimeException("carName was not specified in constructor");
        }
//        System.out.println(">> recordValueJson " + recordValueJson);
        Document document = MapRDB.newDocument(recordValueJson);
        telemetryTable.insert(UUID.randomUUID().toString(), document);
//        System.out.println(">> inserted " + recordValueJson);
        telemetryTable.flush();
//        System.out.println(">> flushed " + recordValueJson);

//        telemetryTable.find().iterator().forEachRemaining(new Consumer<Document>() {
//            @Override
//            public void accept(Document entries) {
//                System.out.println(">> " + entries.asJsonString());
//            }
//        });
    }

    /**
     * Returns the latest by timestamp race
     * @return race object
     */
    public Race getLatestRace() throws NoRacesException {
        QueryCondition allRacesCondition = MapRDB.newCondition()
                .is("_type", QueryCondition.Op.EQUAL, "race")
                .build();
        List<Document> sortedRaces = StreamSupport.stream(
                racesTable.find(allRacesCondition).spliterator(), false
            ).peek(entries -> System.out.println(entries.asJsonString()))
                .sorted(
                Collections.reverseOrder(
                        (d1, d2) -> ((Double)d1.getDouble("timestamp")).compareTo(d2.getDouble("timestamp"))
                )
            ).collect(Collectors.toList());
        if (sortedRaces.size() == 0) {
            throw new NoRacesException("No races found");
        }

        return documentToRace(sortedRaces.get(0));
    }

    /**
     * Returns set of records ordered by race time starting from the offset
     * @param raceTimestamp race ID (timestamp)
     * @param offset offset in seconds from race start
     * @return records stream
     */
    public Stream<TelemetryRecord> getRecords(double raceTimestamp, double offset) {
        QueryCondition raceValues = MapRDB.newCondition()
                .and()
                .is("timestamp", QueryCondition.Op.EQUAL, raceTimestamp)
                .is("racetime", QueryCondition.Op.GREATER, offset)
                .close()
                .build();
        Stream<TelemetryRecord> sortedValues =
                StreamSupport.stream(telemetryTable.find(raceValues).spliterator(), false)
                .sorted(
                        (d1, d2) -> ((Double)d1.getDouble("racetime")).compareTo(d2.getDouble("racetime"))
                )
                .map((o) -> (documentToRecord(o)));
        return sortedValues;
//        return null;
    }

    private static Race documentToRace(Document doc) {
        Race race = new Race();
//        System.out.println("document to race: " + doc.asMap());
        try {
            BeanUtils.populate(race, doc.asMap());
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Can not convert race document");
        }
        System.out.println("built race: " + race.getTimestamp());
        return race;
    }

    private static TelemetryRecord documentToRecord(Document doc) {
        TelemetryRecord record = new TelemetryRecord();
        Map<String, Object> map = doc.asMap();
        Map<String, Object> sensors = (Map<String, Object>) map.get("sensors");
        map.remove("sensors");
        try {
            BeanUtils.populate(record, map);

            TelemetryData telemetryData = new TelemetryData();

            for(String key : Lists.newArrayList((sensors.keySet()))) {
                Object value = sensors.get(key);
                sensors.remove(key);
                sensors.put(key.toLowerCase(), value);
            }
            BeanUtils.populate(telemetryData, sensors);

            record.setSensors(telemetryData);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Can not convert record document");
        }
        return record;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
