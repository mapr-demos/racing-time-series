package com.mapr.examples.telemetryagent.beans;

import java.util.Collection;

public class Race {
    private double timestamp;
    private int carsCount;
    private Collection<Integer> carIds;

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    public int getCarsCount() {
        return carsCount;
    }

    public void setCarsCount(int carsCount) {
        this.carsCount = carsCount;
    }

    public Collection<Integer> getCarIds() {
        return carIds;
    }

    public void setCarIds(Collection<Integer> carIds) {
        this.carIds = carIds;
    }
}
