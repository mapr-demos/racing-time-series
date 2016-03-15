package com.mapr.samples.racingseries.dto;

import java.util.List;

public class TelemetryTimestamp {
	private double time;
	private List<CarTelemetryDataPoint> cars;

	public TelemetryTimestamp(double time, List<CarTelemetryDataPoint> cars) {
		this.time = time;
		this.cars = cars;
	}

	public double getTime() {
		return time;
	}

	public void setTime(double time) {
		this.time = time;
	}

	public List<CarTelemetryDataPoint> getCars() {
		return cars;
	}

	public void setCars(List<CarTelemetryDataPoint> cars) {
		this.cars = cars;
	}
}
