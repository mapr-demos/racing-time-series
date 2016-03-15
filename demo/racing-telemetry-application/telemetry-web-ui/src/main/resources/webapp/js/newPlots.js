var plots = [];
var margin = {
  top : 30,
  right : 100,
  bottom : 30,
  left : 50
}, width = 700 - margin.left - margin.right, height = 400 - margin.top
    - margin.bottom;
var duration = 700;
var checkedLegendIcon = '\uf14a';
var uncheckedLegendIcon = '\uf096';

function PlotCarsTelemetryValueByTime(name, telemetryParameter, elBindTo,
    yAxisLabel, type) {
  this.name = name;
  this.telemetryParameter = telemetryParameter;
  this.elementBindTo = elBindTo;
  this.hiddenCars = [];
  this.hiddenCarsLaps = [];
  this.yAxisLabel = yAxisLabel;
  this.type = type; // undefined or 'time' - by time, 'ax' - by lap distance
  this.init();
}

PlotCarsTelemetryValueByTime.prototype.rescaleYAxis = function() {
  var hiddenCars = this.hiddenCars;
  var sensorKey = this.telemetryParameter;
  var notHiddenCars = newData.filter(function(item) {
    return hiddenCars.indexOf(item.name) < 0;
  });
  var domainValues = notHiddenCars.map(function(carTimestamps) {
    return d3.max(carTimestamps.timestamps, function(sensorsTimestamp) {
      return sensorsTimestamp.sensors[sensorKey];
    });
  });
  var maxValue = d3.max(domainValues);
  this.y.domain([ 0, maxValue ]);
}

PlotCarsTelemetryValueByTime.prototype.isAxType = function() {
  return this.type === 'distance';
}

PlotCarsTelemetryValueByTime.prototype.rescaleXAxis = function() {
  var self = this;
  var domainValues;
  if (!self.isAxType()) {
    domainValues = newData.map(function(carTimestamps) {
      return d3.extent(carTimestamps.timestamps, function(sensorsTimestamp) {
        return sensorsTimestamp.timestamp;
      });
    });
  } else {
    domainValues = newData.map(function(carItem) {
      return carItem.laps.filter(function(lap, i){
        return self.carLapIsVisible(carItem.name, i + 1);
      }).map(function(lapSensors, i) {
          return d3.extent(lapSensors, function(sensor) {return sensor.distance});
      });
    }).map(function(extent){
      return [0, d3.max(extent, function(item){ return item[1];})];
    });
  }

  var minValue = d3.min(domainValues, function(extent) {
    return extent[0];
  });
  var maxValue = d3.max(domainValues, function(extent) {
    return extent[1];
  });
  this.x.domain([ minValue, maxValue ]);
}

PlotCarsTelemetryValueByTime.prototype.carIsVisible = function(carId) {
  return this.hiddenCars.indexOf(carId) < 0;
}

PlotCarsTelemetryValueByTime.prototype.carLapIsVisible = function(carId, lap) {
  return this.hiddenCarsLaps.indexOf(this.getCarLapPathId(carId, lap)) < 0;
}

PlotCarsTelemetryValueByTime.prototype.getCarPathId = function(carId) {
  return 'car' + carId;
}

PlotCarsTelemetryValueByTime.prototype.getCarLapPathId = function(carId, lap) {
  return 'car' + carId + '-lap-' + lap;
}

PlotCarsTelemetryValueByTime.prototype.getLegendItemId = function(carId, lap) {
  var legendItemId = this.name + '-legend-';
  if(this.isAxType()) {
    legendItemId += this.getCarLapPathId(carId, lap);
  } else {
    legendItemId += this.getCarPathId(carId);
  }
  return legendItemId;
}

PlotCarsTelemetryValueByTime.prototype.init = function() {
  var self = this;
  self.color = self.isAxType() ? d3.scale.category20() : d3.scale.category10();
  self.x = d3.scale.linear().range([ 0, width ]);
  self.rescaleXAxis();

  self.y = d3.scale.linear().range([ height, 0 ]);
  self.rescaleYAxis();
  self.line = d3.svg.line().interpolate('linear').x(function(d) {
    return self.x(self.isAxType() ? d.distance : d.timestamp);
  }).y(function(d) {
    return self.y(self.isAxType() ? d[self.telemetryParameter] : d.sensors[self.telemetryParameter]);
  });

  self.svg = d3
  .select(self.elementBindTo)
  .append('svg')
  .attr('class', 'chart')
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  self.xAxis = self.svg
  .append('g')
  .attr('class', 'x axis')
  .attr('transform', 'translate(0,' + height + ')')
  .call(self.x.axis = d3.svg.axis().scale(self.x).orient('bottom'));

  self.yAxis = self.svg
      .append('g')
      .attr('class', 'y axis')
      .call(self.y.axis = d3.svg.axis().scale(self.y).orient('left'));

  self.paths = self.svg.append('g');

  if (self.isAxType()) {
    self.addOrUpdateCarLapPathes();
  } else {
    newData.forEach(function(carItem){self.addOrUpdateCarPath(carItem)});
  }
  self.svg
  .append("text")
  .attr('class', 'axis-label y-axis-label')
  .attr("transform", "rotate(-90)")
  .attr("y", 0 - margin.left).attr("x", 0 - (height / 2))
  .attr("dy", "1em")
  .text(self.yAxisLabel);

  self.svg
  .append("text")
  .attr('class', 'axis-label')
  .attr("transform", "translate(" + (width / 2) + " ," + (height + margin.bottom) + ")")
  .text(self.isAxType() ? "Lap distance, m" : "Time, s");
}

PlotCarsTelemetryValueByTime.prototype.addOrUpdateCarPath = function(carItem) {
  var self = this;
  if(!self.carIsVisible(carItem.name)) {
    return;
  }
  var carPath = d3.select(self.elementBindTo + ' #' + self.getCarPathId(carItem.name));
  if(carPath.empty()) {
    if(self.color.domain().indexOf(carItem.name) === -1) {
      self.color(carItem.name);
    }
      self.paths
      .append('path')
      .attr('class', 'group')
      .attr('id', self.getCarPathId(carItem.name))
      .attr("d", self.line(carItem.timestamps))
      .style('stroke', self.color(carItem.name));

      var currentLegendsCount = d3.selectAll(self.elementBindTo + ' .legend').size();

      var legend = self.svg
      .append("text")
      .attr("id",
          self.getLegendItemId(carItem.name))
          .attr("x", width + margin.left)
          .attr("y", margin.top + currentLegendsCount * 20)
          .attr("class", "legend legend-active")
          .style("fill", self.color(carItem.name))
          .on("click",
              function() {
                var idx = self.hiddenCars.indexOf(carItem.name);
                if (idx < 0 && newData.length - self.hiddenCars.length <= 1) {
                  // prevent last graph hiding
                  return;
                }
                d3.select(self.elementBindTo + " #" + self.getCarPathId(carItem.name))
                .classed("hidden", idx < 0);
                var legendItemSelector = self.elementBindTo + " #" + this.id;
                d3.select(legendItemSelector)
                .classed('legend-active', idx > -1);

                d3.select(legendItemSelector + ' .legend-icon')
                .text(idx > -1 ? checkedLegendIcon : uncheckedLegendIcon);

                if (idx > -1) {
                  self.hiddenCars.splice(idx, 1);
                } else {
                  self.hiddenCars.push(carItem.name);
                }

              });
      legend
      .append("tspan")
      .attr('class', 'legend-icon')
      .text(checkedLegendIcon);

      legend
      .append("tspan")
      .attr('class', 'legend-car-name')
      .attr('dx', '5')
      .text('Car ' + carItem.name);

  } else {
    carPath.attr("d", self.line(carItem.timestamps));
  }

}

PlotCarsTelemetryValueByTime.prototype.addOrUpdateCarLapPathes = function() {
  var self = this;
  function collectVisibleCarLaps(result, carItem) {
    var carId = carItem.name;

    var lapsHash = {};

    carItem.laps.forEach(function(lapData, i){
      var lap = i +1;
      if(!self.carLapIsVisible(carId, lap)) {
        return;
      }
      lapsHash[lap] = lapData;
    });

    if (Object.keys(lapsHash).length) {
      result.push({carId: carId, laps: lapsHash});
    }
    return result;
  }

  function refreshOrAddCarLapPath(carId, lap, data) {
    var carPath = d3.select(self.elementBindTo + ' #' + self.getCarLapPathId(carId, lap));
    var carLapKey = self.getCarLapPathId(carId, lap);
    if(carPath.empty()) {

      self.color(carLapKey);

      self.paths
      .append('path')
      .attr('class', 'group')
      .attr('id', self.getCarLapPathId(carId, lap))
      .attr("d", self.line(data))
      .style('stroke', self.color(carLapKey));

      var currentLegendsCount = d3.selectAll(self.elementBindTo + ' .legend').size();

      var legend = self.svg
      .append("text")
      .attr("id",
          self.getLegendItemId(carId, lap))
          .attr("x", width + margin.left)
          .attr("y", margin.top + currentLegendsCount * 20)
          .attr("class", "legend legend-active")
          .style("fill", self.color(carLapKey))
          .on("click",
              function() {
                    var idx = self.hiddenCarsLaps.indexOf(carLapKey);
                    if (idx < 0 && d3.selectAll(self.elementBindTo + ' .legend').size() - self.hiddenCarsLaps.length <= 1) {
                      // prevent last graph hiding
                      return;
                    }
                    d3.select(self.elementBindTo + " #" + carLapKey)
                    .classed("hidden", idx < 0);
                    var legendItemSelector = self.elementBindTo + " #" + this.id;
                    d3.select(legendItemSelector)
                    .classed('legend-active', idx > -1);
                    d3.select(legendItemSelector + ' .legend-icon')
                    .text(idx > -1 ? checkedLegendIcon : uncheckedLegendIcon);

                    if (idx > -1) {
                        self.hiddenCarsLaps.splice(idx, 1);
                    } else {
                      self.hiddenCarsLaps.push(carLapKey);
                    }

                });

        legend
        .append("tspan")
        .attr('class', 'legend-icon')
        .text(checkedLegendIcon);

        legend
        .append("tspan")
        .attr('class', 'legend-car-name')
        .attr('dx', '5')
        .text('Car ' + carId + '| LAP ' + (lap-1));

//        if (d3.select("#" + self.getLegendItemId(carId, lap-1)).size() > 0) {
//            var carLapKey = self.getCarLapPathId(carId, lap-1);
//            var carLegendKey = self.getLegendItemId(carId, lap-1);
//
//            var idx = self.hiddenCarsLaps.indexOf(carLapKey);
//            if (idx < 0) {  // if it's not hidden already
//                d3.select(self.elementBindTo + " #" + carLapKey).classed('hidden', true);
//                d3.select(self.elementBindTo + " #" + carLegendKey).classed('legend-active', false);
//                d3.select(self.elementBindTo + " #" + carLegendKey + ' .legend-icon').text(uncheckedLegendIcon);
//
//                self.hiddenCarsLaps.push(carLapKey);
//            }
//        }

    } else {
      carPath.attr("d", self.line(data));
    }
  }

  var carLaps = newData.reduce(collectVisibleCarLaps, []);
  carLaps.forEach(function(item){
    Object.keys(item.laps).forEach(function(lapKey, i, arr){
      //refresh only latest lap
//      if(i === arr.length - 1) {
        refreshOrAddCarLapPath(item.carId, lapKey, item.laps[lapKey]);
//      }
    });
  });

}

PlotCarsTelemetryValueByTime.prototype.refresh = function(callback) {
  var self = this;
  self.rescaleXAxis();
  self.rescaleYAxis();
  var svg = d3.select(self.elementBindTo).transition();
  if(self.isAxType()) {
    self.addOrUpdateCarLapPathes();
  } else {
    newData.forEach(function(carItem){self.addOrUpdateCarPath(carItem)});
  }
  svg.select(".x.axis").duration(duration).ease('linear').call(self.x.axis);

  svg.select(".y.axis").duration(duration).ease('linear').call(self.y.axis)
      .each('end', callback);
}

PlotCarsTelemetryValueByTime.prototype.toggleVisibility = function() {
  this.visible = !this.visible;
}

window.initGraphCallback = function() {
  plots.push(new PlotCarsTelemetryValueByTime("speed_time_chart", "speed",
      ".speed-time-graph", 'Speed, m/s'));
  plots.push(new PlotCarsTelemetryValueByTime("speed_dist_chart", "speed",
      ".speed-dist-graph", 'Speed, m/s', 'distance'));
    plots.push(new PlotCarsTelemetryValueByTime("rpm_time_chart", "rpm",
        ".rpm-time-graph", 'RPM'));
    plots.push(new PlotCarsTelemetryValueByTime("rpm_dist_chart", "rpm",
        ".rpm-dist-graph", 'RPM', 'distance'));
  tick();
}

function tick() {
  var visiblePlots = plots.filter(function(plot){
    return plot.visible;
  });

  if(visiblePlots.length) {
    visiblePlots.forEach(function(plot, i, arr) {
      plot.refresh(i === arr.length - 1 ? tick : undefined);
    });
  } else {
    setTimeout(tick, duration);
  }
}

function toggleVisibility(chartId) {
  plots.filter(function(plot){return plot.name === chartId;})[0].toggleVisibility();
}