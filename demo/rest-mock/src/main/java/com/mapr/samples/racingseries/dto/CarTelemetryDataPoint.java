package com.mapr.samples.racingseries.dto;

import com.mapr.examples.telemetryagent.beans.TelemetryData;

public class CarTelemetryDataPoint {
	private int carId;
	private TelemetryData sensors;

	public CarTelemetryDataPoint() {
	}

	public CarTelemetryDataPoint(int carId, TelemetryData sensors) {
		this.carId = carId;
		this.sensors = sensors;
	}

	public int getCarId() {
		return carId;
	}

	public void setCarId(int carId) {
		this.carId = carId;
	}

	public TelemetryData getSensors() {
		return sensors;
	}

	public void setSensors(TelemetryData sensors) {
		this.sensors = sensors;
	}

}
