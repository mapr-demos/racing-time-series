package com.mapr.examples.telemetryagent;


import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.json.JSONException;
import org.json.JSONObject;
import org.ojai.Document;

import java.util.UUID;

public class CarsDAO {
    Table table;

    final static public String APPS_DIR = "/apps/";

    public CarsDAO(String topicName) {
        this.table = this.getTable(APPS_DIR + topicName);
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

    public void insert(String recordValue) {
        Document document = MapRDB.newDocument(recordValue);
        table.insert(UUID.randomUUID().toString(), document);

        table.flush();
    }
}
