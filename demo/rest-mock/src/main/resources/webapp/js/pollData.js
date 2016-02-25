window.newData = [];
window.latestTimestamp = 0;

function pollData(callback) {
	d3.json('/telemetry/metrics?offset=' + latestTimestamp,
			function(error, telemerty) {
				if(error) {
					console.log('Error', error);
					return;
				} else {								
				mapTelemertyNew(telemerty);
					if (callback) {
						callback();
					}
				}	
				setTimeout(pollData, 200);
			});
}

function getTimestapsForCar(carId) {
	var carTimestamps = newData.filter(function(carTimestamps) {
		return carTimestamps.name === carId;
	});
	if (carTimestamps.length) {
		return carTimestamps[0].timestamps;
	} else {
		var newCarEntry = {
			name : carId,
			timestamps : []
		};
		newData.push(newCarEntry);
		return newCarEntry.timestamps;
	}
}

function mapTelemertyNew(telemerty) {
	if (telemerty && telemerty.timestamps) {
		telemerty.timestamps.forEach(function(timestampItem) {
			timestampItem.cars.forEach(function(carSensors) {
				var carTimestamps = getTimestapsForCar(carSensors.carId);
				// Search for the already existing car timestamp for this car
				var sensorsTimestamp = carTimestamps.filter(function(
						carSensorsTimestamp) {
					return carSensorsTimestamp === timestampItem.time;
				});
				if (sensorsTimestamp.length) {
					// the timestamp already exists. just update sensors data
					sensorsTimestamp[0].sensors = carTelemetry.sensors;
				} else {
					// add timestamp for the car
					carTimestamps.push({
						timestamp : timestampItem.time,
						sensors : carSensors.sensors
					});
				}
			});
			// update latest timestamp
//			if (timestampItem.time < latestTimestamp) {
//                window.newData = [];
//                window.latestTimestamp = 0;
//			} else {
			    latestTimestamp = timestampItem.time > latestTimestamp ? timestampItem.time : latestTimestamp;
//			}
		});
	}
}

pollData(window.initGraphCallback, window.realTime);