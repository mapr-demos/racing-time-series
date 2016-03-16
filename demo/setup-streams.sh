if [ "`id -un`" != 'mapr' ]; then
	echo "Should be executed as mapr user"
	exit 1
fi

if maprcli stream info -path /apps/racing/stream > /dev/null
then
	echo "> Deleting old /apps/racing/stream/ stream"
	maprcli stream delete -path /apps/racing/stream
fi

if hadoop fs -ls /apps/racing 2>&1 > /dev/null
then
	echo "> Deleting old /apps/racing table and other information"
	hadoop fs -rm -r -f /apps/racing
fi

echo

echo "> Create base dir for Racing Application /apps/racing/ "
hadoop fs -mkdir /apps/racing/


echo "> Create stream -path /apps/racing/stream"
maprcli stream create -path /apps/racing/stream -produceperm p -consumeperm p -topicperm p

echo "> Create topic -path /apps/racing/stream -topic all"
maprcli stream topic create -path /apps/racing/stream -topic all
echo "> Create topic -path /apps/racing/stream  -topic events"
maprcli stream topic create -path /apps/racing/stream -topic events

for i in `seq 1 10`; do
	echo "> Create topic -path /apps/racing/stream -topic car$i"
	maprcli stream topic create -path /apps/racing/stream -topic car$i
done

echo "> Create base dir for MapR DB tables /apps/racing/db/telemetry"
hadoop fs -mkdir -p /apps/racing/db/telemetry

echo "======  Streams and Topics Created"
