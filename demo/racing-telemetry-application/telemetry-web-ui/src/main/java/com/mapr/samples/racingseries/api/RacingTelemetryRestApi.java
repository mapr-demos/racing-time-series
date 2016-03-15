package com.mapr.samples.racingseries.api;

import com.mapr.examples.telemetryagent.CarsDAO;
import com.mapr.examples.telemetryagent.beans.Race;
import com.mapr.examples.telemetryagent.util.NoRacesException;
import com.mapr.samples.racingseries.dto.CarTelemetryDataPoint;
import com.mapr.samples.racingseries.dto.TelemetryTimestamp;
import com.mapr.samples.racingseries.dto.TelemetryTimestampsResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;


@Path("/telemetry/metrics")
public class RacingTelemetryRestApi {
	private final CarsDAO carsDAO = new CarsDAO();
	private final List<CarsDAO> carsDAOs = new ArrayList<>();

	public RacingTelemetryRestApi() {
		for (int i = 0; i < 11; i++) {
			carsDAOs.add(new CarsDAO(i, String.format("car%d", i)));
		}
	}


	@GET
	@Produces(APPLICATION_JSON)
	public TelemetryTimestampsResponse getData(@QueryParam("offset") Double offset) {
		TelemetryTimestampsResponse response = new TelemetryTimestampsResponse();

		if (offset == null) {
			offset = 0.0;
		}

		Race race;
		try {
			race = carsDAO.getLatestRace(); // FIXME: don't retrieve latest race on every call, it's expensive
		} catch (NoRacesException ex) {
			System.err.println("No races found");
			return response;
		}
		System.out.println("Race " + race.getTimestamp());

		final Double finalOffset = offset;
		List<TelemetryTimestamp> timestamps =
				race.getCarIds().parallelStream().flatMap((carId) -> {
					CarsDAO carDAO = carsDAOs.get(carId);
			System.out.println("> car " + carDAO.toString() + ": " + carDAO.getId());

					return carDAO.getRecords(race.getTimestamp(), finalOffset).
							map(telemetryRecord -> {
								ArrayList<CarTelemetryDataPoint> singleCarTimestamp = new ArrayList<>();
								singleCarTimestamp.add(
										new CarTelemetryDataPoint(carDAO.getId(),
												telemetryRecord.getSensors())
								);

								return new TelemetryTimestamp(telemetryRecord.getRacetime(), singleCarTimestamp);
							});
				}).
						sorted((o1, o2) -> ((Double) o1.getTime()).compareTo(o2.getTime())).
						collect(Collectors.toList());

		response.setTimestamps(timestamps);
		response.setRaceId(race.getTimestamp());
		return response;
	}
}
