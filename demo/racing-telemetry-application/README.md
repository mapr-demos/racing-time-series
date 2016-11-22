# Demonstration 

## Running a Race and Show Real Time Metrics

1. Open the Telemetry Web Application that is running on the Vagrant VM, where TORCS is running on port 8080, something like

```
http://192.168.56.102:8080/demo.html
```

2. In TORCS run a new race

Note that only the cars "inferno" are sending metrics to MapR Streams

3. In the Telemetry Web Application you should see information coming and drawing graphs such as:

* Speed by time in m/s
* Speed by distance and lap in m/s
* RPM by time 
* ROM by distance and lap

Note concerting laps:

The LAP0 shows the data of the cars sent before they pass the start line.


## Analyze Data with SQL (Apache Drill)

1. Copy the `/demo/cars.json` file, into the `/apps/racing/` folder of your MapR Cluster, and be sure that cars.json is accessible using mapr user

2. Sample queries to do on the data:


*Sample Data*

```
SELECT *
FROM dfs.`/apps/racing/db/telemetry/all_cars`
LIMIT 10
```

*Number of races by car*

```
SELECT car, count(1)
FROM (
  SELECT car, race_id
  FROM dfs.`/apps/racing/db/telemetry/all_cars`
  GROUP BY car, race_id
)
GROUP BY CAR
```

*Avg Speed by car and race*

```
SELECT race_id, car, ROUND(AVG( `t`.`records`.`sensors`.`Speed` ),2) as `mps`, ROUND(AVG( `t`.`records`.`sensors`.`Speed` * 18 / 5 ), 2) as `kph`
FROM
(
 SELECT race_id, car, flatten(records) as `records`
 FROM dfs.`/apps/racing/db/telemetry/all_cars`
) AS `t`
GROUP BY race_id, car
```

*Cars from Files*

```
SELECT * 
FROM dfs.`/apps/racing/cars.json`
```

*Sensor data and car information*

```
SELECT  t.car, c.model, c.driver, ROUND(AVG( `t`.`records`.`sensors`.`Speed` ),2) as `mps`, ROUND(AVG( `t`.`records`.`sensors`.`Speed` * 18 / 5 ), 2) as `kph`
FROM
(
 SELECT race_id, car, flatten(records) as `records`
 FROM dfs.`/apps/racing/db/telemetry/all_cars`
) AS `t`
JOIN dfs.`/apps/racing/cars.json` as `c` ON c.id = t.car 
GROUP BY  t.car, c.model, c.driver
```


## Process Sensor Data with Flink

In your favorite IDE open the Processing Flink project and run the com.mapr.examples.telemetryagent.CarSpeedAnalyzer class that prints
the average speed of each car for the last 10 seconds


```
```