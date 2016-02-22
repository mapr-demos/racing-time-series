#!/bin/bash
TA_ROOT=/vagrant/TelemetryAgent/
cd $TA_ROOT

java -cp "`mapr classpath`:$TA_ROOT/target/telemetryagent-example.jar" Main -t $1 -c $TA_ROOT/src/main/resources/streams.conf
