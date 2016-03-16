# Racing Time Series
This application shows how to use MapR Converged Data Platform to capture data from Racing Cars for:

* Real Time Analytics
* Big Data Processing

The application is built using various components:

* [The Open Racing Car Simulator](https://sourceforge.net/projects/torcs/) : used to generate metrics data such as speed, rpm, and lap during races
* The car metrics are:
   * captured using a MapR Streams Producer
   * consumed by a consumer that push data in real time into a Web Browser.
   * another consumer is saving all the events into a MapR-DB JSON table for further analytic purpose


## Prerequisites

* MapR 5.1 or later Cluster with MapR-DB and Streams enabled, this could be the [MapR 5.1 Sandbox](https://www.mapr.com/products/mapr-sandbox-hadoop/download)
* [Vagrant](https://www.vagrantup.com/downloads.html), the demonstration is running in a Vagrant Virtual Machine
* Git to clone the demonstration content


## Installation


### 1- Clone the project

```
$ git clone https://github.com/mapr-demos/racing-time-series.git
```

### 2- Create Folders and Streams

The demonstrations is using:

* Topics located in the `/app/racing/stream` stream
* Tables located in the `/apps/racing/db/telemetry` folder

To create all these object run the `./demo/setup-stream.sh` file on your MapR cluster.

Note: you can run this script any time to reset the entire environment. 

### 3- Configure the project

As mentioned above the demonstration will be running in a Vagrant Virtual Machine, that will connect to your MapR Cluster. 
The first thing to do is to configure the project to match your environment. 

Open the `./demo/config.conf` file and edit the following properties:

* `MAPR_UID` and `MAPR_GID` : the mapr user id and mapr group id used by your cluster to be sure the MapR client and cluster are using the same ids
* `CLUSTER_IP` one of the nodes of your cluster
* `CLUSTER_NAME` the name of your MapR cluster

Note: The cluster IP and name will be used to configure the MapR Client, you will be able to modify that later if necessary.

### 4- Initialise the Racing demonstration

You will now use Vagrant to create a new Virtual Machine, define by the `./demo/Vagrantfile`. (You should not have to change the configuration). In a terminal do the following:

```
$ cd demo
$ vagrant up
```

This operation will create a new Ubuntu virtual machine and execute the `demo/setup.sh` script.

The `setup.sh` is executed automatically the first time, and it takes a while, as it will:

* Update Ubuntu
* Install additional package including MapR Client and MapR Stream client (mapr-kafka package)
* Download TORCS source code, the race simulator
* Apply a patch to TORCS to allow the demonstration to capture racing cars telemtry information
* Build TORCS
* Build the Racing Telemetry demonstration itself (Java projects located in `./demo/racing-telemetry-application`)


### 5- Check the installation

You should be able to connect to the VM using the 'vagrant' user with password 'vagrant', you can also connect to the VM using ssh using the command: `vagrant ssh`, when you are in the `./demo/` folder.

In a terminal, in the Ubuntu VM, run the following command:

* `hadoop fs -ls /` : you should see the list of folders of you MapR cluster

If you encounter errors it is most of the time due to some network issues, you must be sure that your Virtual Machine can access the MapR cluster.



## Running the Demonstration

1. If the Ubuntu VM is not running, using a terminal go to `./demo/` and run `vagrant up`

2. Go to the Ubuntu desktop, if the screen is locked, log with the user vagrant password vagrant.

3. On the desktop click on the "Streams Demo" icon, this will start multiple windows.

4. After few seconds you should see the TORCS simulator and a Web Browser 

5. In the TORCS Simulator, click on `Race`,  `Quick Race`, `New Race`

6. This should start a race with 3 cars, and send data to your Web Browser, where you can select various metrics.


Note:

* The current Web site only print the data of the current race
* You can customise the race in TORCS but only the *Inferno* cars are emitting data to Streams.


## Data Flow - How this works?

The application is made of 3 major components:

* The TORCS, that as been updated to emit Inferno cars metrics into a file. `/usr/local/share/games/torcs/telemetry/Inferno.dat` on the Vagrant VM.
* A [Kafka Producer](https://github.com/mapr-demos/racing-time-series/blob/master/demo/racing-telemetry-application/telemetry-agent/src/main/java/com/mapr/examples/telemetryagent/TelemetryProducer.java), that reads the file to post metrics on the `/apps/racing/stream:events` topic. 
* The demonstration uses several Consumers to split information by cars, but also send data in real time to the Web interface using Web socket.
  * [A generic Consumer](https://github.com/mapr-demos/racing-time-series/blob/master/demo/racing-telemetry-application/telemetry-agent/src/main/java/com/mapr/examples/telemetryagent/CarStreamsRouter.java) that split the events to many topics, one generic and one by car
  * [CarStreamConsumer](https://github.com/mapr-demos/racing-time-series/blob/master/demo/racing-telemetry-application/telemetry-agent/src/main/java/com/mapr/examples/telemetryagent/CarStreamConsumer.java) to save data in one table by car (`/apps/racing/db/telemety/car1`, ..., `car10`)
  * A Web application with a [EventSourceServlet](https://github.com/mapr-demos/racing-time-series/blob/master/demo/racing-telemetry-application/telemetry-web-ui/src/main/java/com/mapr/samples/racingseries/api/RealTimeApi.java) that use the [LiveConsumer](https://github.com/mapr-demos/racing-time-series/blob/master/demo/racing-telemetry-application/telemetry-agent/src/main/java/com/mapr/examples/telemetryagent/LiveConsumer.java) to send event to the browser using Websocket. The LiveConsumer subscribe to individual car topics.

The most important message type looks like:

```
{
	"sensors" : 
	{
		"Speed":51.13699,
		"Distance":4051.190186,
		"RPM":706.666504
	}
	,
	"racetime":73.738,
	"timestamp":1458127205
}
```

* Speed in km/h
* Distance in km
* RPM
* Racetime: time in the race
* timestamp: start time of the race








