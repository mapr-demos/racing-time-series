package com.mapr.examples.telemetryagent;


import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.store.exceptions.DocumentNotFoundException;

import java.util.UUID;

public class CarsDAO {
    Table telemetryTable;
    Table racesTable;

    final static public String APPS_DIR = "/apps/telemetry/";
    public static final String RACES_TABLE = APPS_DIR + "races";

    public CarsDAO() {
        this.racesTable = this.getTable(RACES_TABLE);
    }

    public CarsDAO(String carName) {
        this.telemetryTable = this.getTable(APPS_DIR + carName);
        this.racesTable = this.getTable(RACES_TABLE);
    }

    private synchronized int autoincRacesCounter() {
        final String RACES_SAVED_FIELD = "racesSaved";
        final String COUNTER_DOCUMENT_ID = "count";

        Document racesCount;
        try {
            racesCount = this.racesTable.findById(COUNTER_DOCUMENT_ID);
        } catch (DocumentNotFoundException e) {
            racesCount = MapRDB.newDocument();
            racesCount.set(RACES_SAVED_FIELD, 0);
        }

        int counter = racesCount.getInt(RACES_SAVED_FIELD) + 1;
        racesCount.set(RACES_SAVED_FIELD, counter);

        this.racesTable.insertOrReplace(racesCount);
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
        Document document = MapRDB.newDocument(recordValueJson);
        telemetryTable.insert(UUID.randomUUID().toString(), document);

        telemetryTable.flush();
    }

    public void queryOffset() {


    }
}
