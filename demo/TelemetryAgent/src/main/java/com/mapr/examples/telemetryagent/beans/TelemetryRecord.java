package com.mapr.examples.telemetryagent.beans;

public class TelemetryRecord {
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
}
