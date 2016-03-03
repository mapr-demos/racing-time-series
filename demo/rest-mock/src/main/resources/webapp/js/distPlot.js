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

var color = d3.scale.category10();

function PlotCarsTelemetryValueByDistance(name, telemetryParameter, elBindTo,
    yAxisLabel) {
  this.name = name;
  this.telemetryParameter = telemetryParameter;
  this.elementBindTo = elBindTo;
  this.hiddenCars = [];
  this.yAxisLabel = yAxisLabel;
  this.init();
}

PlotCarsTelemetryValueByDistance.prototype.rescaleYAxis = function() {
  var hiddenCars = this.hiddenCars;
  var sensorKey = this.telemetryParameter;
  var notHiddenCars = newData.filter(function(item) {
    return hiddenCars.indexOf(item.name) < 0;
  });
  var domainValues = notHiddenCars.map(function(carTimestamps) {
    return d3.extent(carTimestamps.timestamps, function(sensorsTimestamp) {
      return sensorsTimestamp.sensors[sensorKey];
    });
  });
//  var minValue = d3.min(domainValues, function(extent) {
//    return extent[0];
//  });
  var maxValue = d3.max(domainValues, function(extent) {
    return extent[1];
  });
  this.y.domain([ 0, maxValue ]);
}

PlotCarsTelemetryValueByDistance.prototype.rescaleXAxis = function() {
  var domainValues = newData.map(function(carTimestamps) {
    return d3.extent(carTimestamps.timestamps, function(sensorsTimestamp) {
      return sensorsTimestamp.sensors["distance"];
    });
  });
  var minValue = d3.min(domainValues, function(extent) {
    return extent[0];
  });
  var maxValue = d3.max(domainValues, function(extent) {
    return extent[1];
  });
  this.x.domain([ minValue, maxValue ]);
}

PlotCarsTelemetryValueByDistance.prototype.carIsVisible = function(carId) {
  return this.hiddenCars.indexOf(carId) < 0;
}

PlotCarsTelemetryValueByDistance.prototype.getCarPathId = function(carId) {
  return 'car' + carId;
}

PlotCarsTelemetryValueByDistance.prototype.getLegendItemId = function(carId) {
  return this.name + '_legend_' + this.getCarPathId(carId);
}

PlotCarsTelemetryValueByDistance.prototype.init = function() {
  var self = this;
  self.x = d3.scale.linear().range([ 0, width ]);
  self.rescaleXAxis();

  self.y = d3.scale.linear().range([ height, 0 ]);
  self.rescaleYAxis();
  self.line = d3.svg.line().interpolate('linear').x(function(d) {
    return self.x(d.sensors["distance"]);
  }).y(function(d) {
    return self.y(d.sensors[self.telemetryParameter]);
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
  newData.forEach(function(carItem){self.addOrUpdateCarPath(carItem)});
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
  .text("Distance, m");
}

PlotCarsTelemetryValueByDistance.prototype.addOrUpdateCarPath = function(carItem) {
  var self = this;
  if(!self.carIsVisible(carItem.name)) {
    return;
  }
  var carPath = d3.select(self.elementBindTo + ' #' + self.getCarPathId(carItem.name));
  if(carPath.empty()) {
    if(color.domain().indexOf(carItem.name) === -1) {
      color.domain().push(carItem.name);
    }
      self.paths
      .append('path')
      .attr('class', 'group')
      .attr('id', self.getCarPathId(carItem.name))
      .attr("d", self.line(carItem.timestamps))
      .style('stroke', color(carItem.name));

      var currentLegendsCount = d3.selectAll(self.elementBindTo + ' .legend').size();

      var legend = self.svg
      .append("text")
      .attr("id",
          self.getLegendItemId(carItem.name))
          .attr("x", width + margin.left)
          .attr("y", margin.top + currentLegendsCount * 20)
          .attr("class", "legend legend-active")
          .style("fill", color(carItem.name))
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

PlotCarsTelemetryValueByDistance.prototype.refresh = function(callback) {
  var self = this;
  self.rescaleXAxis();
  self.rescaleYAxis();
  var svg = d3.select(self.elementBindTo).transition();
  newData.forEach(function(carItem){self.addOrUpdateCarPath(carItem)});
  svg.select(".x.axis").duration(duration).ease('linear').call(self.x.axis);

  svg.select(".y.axis").duration(duration).ease('linear').call(self.y.axis)
      .each('end', callback);
}

PlotCarsTelemetryValueByDistance.prototype.toggleVisibility = function() {
  this.visible = !this.visible;
}

window.initGraphDistanceCallback = function() {
  color.domain(newData.map(function(item) {
    return item.name;
  }));
  plots.push(new PlotCarsTelemetryValueByDistance("speed_dist_chart", "speed",
      ".speed-dist-graph", 'Speed, m/s'));
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