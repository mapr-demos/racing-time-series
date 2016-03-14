if [ "`id -un`" != 'mapr' ]; then
	echo "Should be executed as mapr user"
	exit 1
fi

if maprcli stream info -path /cars > /dev/null
then
	echo "> Deleting old /cars stream"
	maprcli stream delete -path /cars
fi

if hadoop fs -ls /apps/telemetry 2>&1 > /dev/null
then
	echo "> Deleting old /apps/telemetry MapR DB tables"
	hadoop fs -rm -r -f /apps/telemetry/
fi

echo

echo "> Create stream -path /cars"
maprcli stream create -path /cars -produceperm p -consumeperm p -topicperm p

echo "> Create topic -path /cars -topic all"
maprcli stream topic create -path /cars -topic all
echo "> Create topic -path /cars -topic events"
maprcli stream topic create -path /cars -topic events

for i in `seq 1 10`; do
	echo "> Create topic -path /cars -topic car$i"
	maprcli stream topic create -path /cars -topic car$i
done

echo "> Create base dir for MapR DB tables /apps/telemetry/"
hadoop fs -mkdir /apps/telemetry/

echo "====== \n Streams and Topics Created"
