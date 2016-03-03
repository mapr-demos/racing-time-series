stream:
	maprcli stream create -path /cars

general:
	maprcli stream topic create -path /cars -topic all
events:
	maprcli stream topic create -path /cars -topic events

cars:
	for i in `seq 1 10`; do \
        	maprcli stream topic create -path /cars -topic car$$i ; \
	done

all: stream general events cars
	hadoop fs -mkdir /apps/telemetry/

clean:
	maprcli stream delete -path /cars
	hadoop fs -rm -r -f /apps/telemetry/
