#!/bin/bash
TA_ROOT=/vagrant/TelemetryAgent/
cd $TA_ROOT

java -cp "`mapr classpath`:$TA_ROOT/target/telemetryagent-example.jar" Main -t $@ -c $TA_ROOT/src/main/resources/streams.conf
