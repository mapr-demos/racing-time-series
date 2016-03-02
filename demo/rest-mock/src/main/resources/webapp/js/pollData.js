window.newData = [];
window.latestTimestamp = 0;
window.raceId = 0;

//function pollData(callback) {
//	d3.json('/telemetry/metrics?offset=' + latestTimestamp,
//			function(error, telemerty) {
//				if(error) {
//					console.log('Error', error);
//					return;
//				} else {
//				mapTelemertyNew(telemerty);
//					if (callback) {
//						callback();
//					}
//				}
//				setTimeout(pollData, 200);
//			});
//}
//
$(function() {
    window.initGraphCallback();
});

var source = new EventSource('/talk');
source.addEventListener('open', function(e) {
    console.log('Connected');
}, false);

source.addEventListener('raceStarted', function(e) {
    newData = [];
}, false);


source.onmessage = function(e) {
    mapTelemertyNew(JSON.parse(e.data));
};

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
    if (telemerty && telemerty.raceId != window.raceId) {
        window.latestTimestamp = 0; // new race started
        window.newData.splice(0, window.newData.length);
        window.raceId = telemerty.raceId;
    }
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
    	    latestTimestamp = timestampItem.time > latestTimestamp ? timestampItem.time : latestTimestamp;
		});
	}
}
