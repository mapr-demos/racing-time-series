#!/bin/bash

cd rest-mock
java -Xdebug -agentlib:jdwp=transport=dt_socket,address=9999,server=y,suspend=n -cp "/opt/mapr/conf:/opt/mapr/lib/*:target/telemetryui.jar" com.mapr.samples.racingseries.Main
