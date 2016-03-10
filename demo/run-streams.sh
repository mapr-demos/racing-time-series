#!/bin/bash
if [ "`id -un`" != 'mapr' ]; then
	echo "Should be executed as mapr user"
	exit 1
fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

echo Starting producer...
./run-stream-agent.sh producer -r true &

echo Starting router...
./run-stream-agent.sh router &

echo Starting events...
./run-stream-agent.sh events &

echo Starting consumer...
./run-stream-agent.sh consumer &


echo Started jobs:
jobs -pr

wait
