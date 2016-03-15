#!/bin/bash

cd /vagrant/racing-telemetry-application/telemetry-web-ui
java -Xdebug -agentlib:jdwp=transport=dt_socket,address=9999,server=y,suspend=n -cp "/opt/mapr/conf:/opt/mapr/lib/*:target/telemetry-web-ui-complete.jar" com.mapr.samples.racingseries.Main
