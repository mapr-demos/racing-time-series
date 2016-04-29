package com.mapr.examples.telemetryagent.beans;

import java.text.SimpleDateFormat;

public class TelemetryRecord {
    private double carId;
    private long timestamp;
    private double racetime;
    private TelemetryData sensors;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getRacetime() {
        return racetime;
    }

    public void setRacetime(double racetime) {
        this.racetime = racetime;
    }

    public TelemetryData getSensors() {
        return sensors;
    }

    public void setSensors(TelemetryData sensors) {
        this.sensors = sensors;
    }

    public double getCarId() {
        return carId;
    }

    public void setCarId(double carId) {
        this.carId = carId;
    }
}
