package com.mapr.samples.racingseries.api;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.google.common.collect.Lists;
import com.mapr.examples.telemetryagent.CarsDAO;
import com.mapr.examples.telemetryagent.beans.TelemetryRecord;
import com.mapr.examples.telemetryagent.beans.Race;
import com.mapr.examples.telemetryagent.util.NoRacesException;
import com.mapr.samples.racingseries.dto.CarTelemetryDataPoint;
import com.mapr.samples.racingseries.dto.TelemetryTimestamp;
import com.mapr.samples.racingseries.dto.TelemetryTimestampsResponse;


@Path("/telemetry/metrics")
public class RacingTelemetryRestApi {
	public RacingTelemetryRestApi() {
		for(int i=0; i<carsDAOs.length; i++) {
			carsDAOs[i] = new CarsDAO(i, String.format("car%d", i));
		}
	}

	private final CarsDAO carsDAO = new CarsDAO();
	private final CarsDAO[] carsDAOs = new CarsDAO[10];

	private int minValue = 1;
	private int maxValue = 150;


//	@GET
//	@Produces(APPLICATION_JSON)
//	public TelemetryTimestampsResponse getData(@QueryParam("offset") Double offset) {
//		TelemetryTimestampsResponse response = new TelemetryTimestampsResponse();
//		List<TelemetryTimestamp> timestamps = new ArrayList<>();
//		if (offset == null) {
//			offset = 0.0;
//		}
//		for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 5); i++) {
//			TelemetryTimestamp timestamp = new TelemetryTimestamp();
//			offset += ThreadLocalRandom.current().nextDouble(0.1, 2);
//			timestamp.setTime(new Double(offset));
//			List<CarTelemetryDataPoint> carPoints = new ArrayList<>();
//			for (int j = 0; j < 4; j++) {
//				carPoints.add(new CarTelemetryDataPoint(j, simulateTelemetryData(j)));
//			}
//			timestamp.setCars(carPoints);
//			timestamps.add(timestamp);
//		}
//		response.setTimestamps(timestamps);
//		return response;
//	}

	@GET
	@Produces(APPLICATION_JSON)
	public TelemetryTimestampsResponse getData(@QueryParam("offset") Double offset) {
		TelemetryTimestampsResponse response = new TelemetryTimestampsResponse();
//		List<TelemetryTimestamp> timestamps = new ArrayList<>();
		if (offset == null) {
			offset = 0.0;
		}
//		for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 5); i++) {
//			TelemetryTimestamp timestamp = new TelemetryTimestamp();
//			offset += ThreadLocalRandom.current().nextDouble(0.1, 2);
//			timestamp.setTime(new Double(offset));
//			List<CarTelemetryDataPoint> carPoints = new ArrayList<>();
//			for (int j = 0; j < 4; j++) {
//				carPoints.add(new CarTelemetryDataPoint(j, simulateTelemetryData(j)));
//			}
//			timestamp.setCars(carPoints);
//			timestamps.add(timestamp);
//		}
		Race race;
		try {
			race = carsDAO.getLatestRace(); // FIXME: don't retrieve latest race on every call, it's expensive
		} catch (NoRacesException ex) {
			System.err.println("No races found");
			return response;
		}
		System.out.println("Race " + race.getTimestamp());
		List<TelemetryTimestamp> timestamps = new ArrayList<>();
		for (CarsDAO carDAO : carsDAOs) {
			System.out.println("> car " + carDAO.toString() + ": " + carDAO.getId());

			List<TelemetryTimestamp> singleCarPoints =
				carDAO.getRecords(race.getTimestamp(), offset).
						map(telemetryRecord -> {
                            ArrayList<CarTelemetryDataPoint> singleCarTimestamp = new ArrayList<>();
                            singleCarTimestamp.add(
                                    new CarTelemetryDataPoint(carDAO.getId(),
                                                              telemetryRecord.getSensors())
                            );

                            return new TelemetryTimestamp(telemetryRecord.getRacetime(), singleCarTimestamp);
                        }).
						collect(Collectors.toList());


			timestamps.addAll(singleCarPoints);
		}
		response.setTimestamps(timestamps);
		return response;
	}

//	@GET
//	@Path("test")
//	public void getTest() {
//		Table racesTable = MapRDB.getTable("/apps/telemetry/races");
//		carsDAO.newRace("{\"test\": 1}");
//		racesTable.find().iterator().forEachRemaining(new Consumer<Document>() {
//			@Override
//			public void accept(Document entries) {
//				System.out.println(entries.asJsonString());
//			}
//		});
//	}

	private double getRandomDouble() {
		return ThreadLocalRandom.current().nextDouble(minValue, maxValue);
	}

	private int getRandomInt() {
		return ThreadLocalRandom.current().nextInt(minValue, maxValue);
	}
}
