#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

rm -f /usr/local/share/games/torcs/telemetry/Inferno.dat

xterm -hold -e "cd /vagrant; sudo -u mapr bash ./run-streams.sh" &
xterm -hold -e "cd /vagrant; sudo -u mapr bash ./run-ui.sh" &
sleep 3
while ! sudo netstat -npl | grep 8080 2>&1 1>/dev/null; do
	sleep 1
	echo "Waiting for web server to start..."
done
firefox 127.0.0.1:8080/demo.html &
sleep 1
torcs &

echo Started jobs:
jobs -pr

wait
