window.newData = [];
window.latestTimestamp = 0;
window.raceId = 0;

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

source.addEventListener('test', function(e) {
    console.log(e.data);
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

function getLatestLapForCar(carId) {
  var car;
  newData.some(function(carItem) {
    if(carItem.name === carId) {
      car = carItem;
      return true;
    }
    return false;
  });
  var laps = car.laps;
  if (!laps) {
    var lap = [];
    car.laps = [lap];
    return lap;
  }
  return laps[laps.length-1];
}

function mapTelemertyNew(telemerty) {
    if (telemerty && telemerty.raceId != window.raceId) {
        window.latestTimestamp = 0; // new race started
        window.newData = [];
        window.raceId = telemerty.raceId;
    }
	if (telemerty && telemerty.timestamps) {
		telemerty.timestamps.forEach(function(timestampItem) {
			timestampItem.cars.forEach(function(carSensors) {
				var carTimestamps = getTimestapsForCar(carSensors.carId);
				var latestLap = getLatestLapForCar(carSensors.carId);
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
				if(latestLap.length) {
				  var lastAx = latestLap[latestLap.length -1].distance;
				  if(carSensors.sensors.distance < lastAx) {
				    var carLapsData;
				    newData.some(function(carItem){
				      if(carItem.name === carSensors.carId) {
				        carLapsData = carItem.laps;
				        return true;
				      }
				      return false;
				    });
				    // Add new lap here
				    carLapsData.push([carSensors.sensors]);
				  } else {
				    latestLap.push(carSensors.sensors);
				  }
				} else {
				  latestLap.push(carSensors.sensors);
				}
			});
			// update latest timestamp
			latestTimestamp = timestampItem.time > latestTimestamp ? timestampItem.time : latestTimestamp;
		});
	}
}
