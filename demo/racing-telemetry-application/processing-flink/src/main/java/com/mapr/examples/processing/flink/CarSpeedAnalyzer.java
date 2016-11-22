package com.mapr.examples.processing.flink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * Simple Flink Job that analyze car sensor data from MapR Streams/Kafka
 */
public class CarSpeedAnalyzer {



  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092"); // not used by MapR Streams
    properties.setProperty("group.id", "avg-speed-service");
    //properties.setProperty("auto.offset.reset", "earliest");


    // event :  {"sensors":{"Speed":25.748163,"Distance":805.477051,"RPM":673.18335},"race_id":"20161110053622","racetime":96.566,"id":3,"timestamp":1478756182}
    DataStream stream =  env.addSource(
                    new FlinkKafkaConsumer09<>("/apps/racing/stream:sensors_data",
                    new JSONDeserializationSchema(),
                    properties)
    );


    stream  .flatMap(new CarEventParser())
            .keyBy(0)
            .timeWindow(Time.seconds(10))
            .reduce(new AvgReducer())
            .flatMap(new AvgMapper())
           .map(new AvgPrinter())
            .print();

    env.execute();
  }


}


/**
 * Get JSON element carid, speed and counter into tuple
 */
class CarEventParser implements FlatMapFunction<ObjectNode, Tuple3<String, Float, Integer>> {
  @Override
  public void flatMap(ObjectNode jsonEvent, Collector<Tuple3<String, Float, Integer>> out) throws Exception {
    // get elements
    String carId = "car" + jsonEvent.get("id").asText();
    float speed = jsonEvent.get("sensors").get("Speed").floatValue() * 18f/5f; // convert to kph
    out.collect(new Tuple3<>(carId,  speed, 1 ));
  }
}


/**
 * Add speed and count to prepare average
 */
class AvgReducer implements ReduceFunction<Tuple3<String, Float, Integer>> {
  @Override
  public Tuple3<String, Float, Integer> reduce(Tuple3<String, Float,Integer> value1, Tuple3<String, Float, Integer> value2) {
    return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2+1);
  }
}

/**
 * Calculate average from tuple
 */
class AvgMapper implements FlatMapFunction<Tuple3<String, Float, Integer>, Tuple2<String, Float>> {
  @Override
  public void flatMap(Tuple3<String, Float, Integer> carInfo, Collector<Tuple2<String, Float>> out) throws Exception {
    out.collect(  new Tuple2<>( carInfo.f0 , carInfo.f1/carInfo.f2 )  );
  }
}


class AvgPrinter implements MapFunction<Tuple2<String, Float>, String> {


  @Override
  public String map(Tuple2<String, Float> avgEntry) throws Exception {
    return  String.format("Avg speed for %s : %.2f km/h ", avgEntry.f0 , avgEntry.f1 ) ;
  }
}