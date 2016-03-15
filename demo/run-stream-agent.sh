#!/bin/bash
TA_ROOT=/vagrant/racing-telemetry-application/telemetry-agent
cd $TA_ROOT

java -cp "`mapr classpath`:$TA_ROOT/target/telemetry-agent.jar" Main -t $@ -c $TA_ROOT/src/main/resources/streams.conf

