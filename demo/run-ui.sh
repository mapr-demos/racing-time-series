#!/bin/bash

cd rest-mock
java -Xdebug -agentlib:jdwp=transport=dt_socket,address=9999,server=y,suspend=n -cp "target/telemetryui.jar" com.mapr.samples.racingseries.Main
