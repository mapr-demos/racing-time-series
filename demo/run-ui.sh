#!/bin/bash

cd rest-mock
java -Xdebug -agentlib:jdwp=transport=dt_socket,address=9999,server=y,suspend=n -cp "/opt/mapr/conf:/opt/mapr/lib/mapr-streams-5.1.0-mapr-SNAPSHOT.jar:/opt/mapr/lib/kafka-clients-0.9.0.0-mapr-SNAPSHOT.jar:/opt/mapr/lib/maprfs-5.1.0-mapr-SNAPSHOT.jar:target/telemetryui.jar" com.mapr.samples.racingseries.Main
